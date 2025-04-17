use std::hint;

use crate::u8p;
use crate::micro_util;
use crate::adaptive_prefix_map::AdaptivePrefixMap;


#[derive(Debug)]
#[allow(non_camel_case_types)]
pub enum FieldType {
    STRUCT(Box<AdaptivePrefixMap<Field>>),

    DATE, // no timezone
    DATETIME, // no timezone
    TIME, // no date or timezone
    TIMESTAMP, // date & time, optionally with timezone

    BOOL,
    INT64,
    FLOAT64,
    DECIMAL_29_9, // up to 29 digits before the decimal point, and up to 9 after (aka NUMERIC in BigQuery)
    
    STRING,
    
    BYTES,
    
    ANY // if incompatible data that can't be struct. or insufficient overlap in fields to treat as a struct (aka JSON in BigQuery)
    // not supported here:
    // GEOGRAPHY
    // INTERVAL
    // RANGE
    // BIGNUMERIC
}


#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum FieldMode {
    #[default]
    NULLABLE,
    REQUIRED,
    REPEATED
}


#[derive(Debug)]
pub struct Field {
    pub idx: usize,
    pub name: String,
    pub mode: FieldMode,
    pub type_: FieldType,
}

#[derive(Debug)]
struct StackEntry<'a> {
    schema: &'a AdaptivePrefixMap<Field>,
    mode: FieldMode,
    is_initialised: bool
}

#[derive(Debug, PartialEq, Eq)]
pub struct ByteOffset(usize);

#[derive(Debug, PartialEq, Eq)]
pub enum ValidationResult<'schema, 'json> {
    Valid,
    FieldUnrecognised(ByteOffset, &'json str),
    FieldDuplicated(ByteOffset, &'schema str),
    RequiredFieldAbsent(ByteOffset, &'schema str),
    RepeatedFieldIsNotArray(ByteOffset, &'schema str),
    ArrayContentsInvalid(ByteOffset, &'schema str),
    FieldValueInvalid(ByteOffset, &'schema str),
    RequiredFieldIsNull(ByteOffset, &'schema str),
    NotAnObject(ByteOffset)
}

/// The validate function needs some heap data, but we don't want to reallocate on every call, so we wrap it up with this
#[derive(Default)]
pub struct ValidateScratch<'a> {
    seen_field_by_idx: Vec<bool>,
    stack: Vec<StackEntry<'a>>
}


/// Validates a line of json according to the `root_schema` uses `scratch` to avoid reallocating on the heap over repeated calls
pub fn validate<'a, 'b>(root_schema: &'a AdaptivePrefixMap<Field>, max_field_idx: usize, json: &'b u8p::u8p, scratch: &mut ValidateScratch<'a>) -> ValidationResult<'a, 'b> {
    let seen_field_by_idx = &mut scratch.seen_field_by_idx;
    let stack = &mut scratch.stack;

    seen_field_by_idx.clear();
    seen_field_by_idx.resize( max_field_idx + 1, false);
    stack.clear();

    let mut j = 0;
    stack.push(StackEntry{schema: &root_schema, mode: FieldMode::REQUIRED, is_initialised: false}); 

    // consume initial whitespace
    j += micro_util::consume_punct::<0, true>(&json);

    'stack_loop: while let Some(stack_entry) = stack.last_mut() {

        if !stack_entry.is_initialised {
            // PRIOR_WHITSPACE:false is allowed because for the first iteration we have already consumed initial whitespace above. For subsequent iterations, the
            // the body of the loop ends with j+= consume_punct() for the default case, and this also happens prior to the one "continue 'stack_loop" statement.
            let j_inc = micro_util::consume_punct::<b'{', false>(&json.offset(j));
            if j_inc == 0 {
                return ValidationResult::NotAnObject(ByteOffset(j)); // this is still potentially spec-compliant JSON, but not meeting our schema's requirements
            }
            j += j_inc;
            stack_entry.is_initialised = true;
        }

        while *json.raw_u8s().get(j).unwrap_or(&0) == b'"' { /* for each member */
            // consume opening quote for key name
            j += 1;

            let Some(field) = stack_entry.schema.get_from_prefix(&json.offset(j)) else { 
                let str_len = micro_util::consume_string(&json.offset(j-1));
                let s = std::str::from_utf8(&json.raw_u8s()[j..j+str_len-2]).unwrap_or("<invalid>");
                return ValidationResult::FieldUnrecognised(ByteOffset(j), s); 
            };

            unsafe {
                // SAFETY: seen_field_by_idx.len() set to max_field_idx+1 above.
                //         It's the responsibility of the caller to enure that max_field_idx is set correctly.
                hint::assert_unchecked(field.idx < seen_field_by_idx.len());
            }

            // Having duplicate keys in an object is, it seems, spec-compliant, so we have to assume there may be duplicates, which would be invalid for us
            if seen_field_by_idx[field.idx] {
                return ValidationResult::FieldDuplicated(ByteOffset(j), &field.name);
            }

            seen_field_by_idx[field.idx] = true;

            j += field.name.as_bytes().len() + 1; // consume field name and closing double quote 
            
            // consume colon - given we're assuming json is valid we don't check the return value
            j += micro_util::consume_punct::<b':', true>(&json.offset(j));

            // consume value
            if let FieldType::STRUCT(sub_schema) = &field.type_ {
                // Struct: unless null, we will need to grow the stack and step into the new level.
                let j_inc_null = micro_util::consume_null(&json.offset(j));
                let j_inc_bra = micro_util::consume_punct::<b'[', false>(&json.offset(j));
                if j_inc_null > 0 {
                    if field.mode == FieldMode::REQUIRED {
                        return ValidationResult::RequiredFieldIsNull(ByteOffset(j), &field.name);
                    }
                    j += j_inc_null;
                } else {
                    if field.mode == FieldMode::REPEATED {
                        // colon was last thing consumed, so if this consumes anything it can only be the actual punctuation we are looking for
                        if j_inc_bra == 0 {
                            return ValidationResult::RepeatedFieldIsNotArray(ByteOffset(j), &field.name);
                        } else {
                            // consume open array
                            j += j_inc_bra;
                        }
                    }

                    let j_inc_close_array = micro_util::consume_punct::<b']', false>(&json.offset(j));
                    if j_inc_close_array == 0 {
                        stack.push(StackEntry{schema: sub_schema.as_ref(), mode: field.mode, is_initialised: false});
                        continue 'stack_loop;
                    } else {
                        j += j_inc_close_array;
                    }
                }
            } else {
                // A singelton/repeated value or null

                // We are carefull before & after the loop to avoid branching on nulls because nulls are valid, but will be hard for the branch predictor to predict.
                let mut json_offset = json.offset(j);
                let j_inc_null = micro_util::consume_null(&json_offset);
                if field.mode == FieldMode::REPEATED { 
                    let j_inc_bra =  micro_util::consume_punct::<b'[', false>(&json_offset);
                    // Cases:
                    // A1. j_inc_null != 0, j_inc_bra can only be 0 (because it was starting from an 'n').
                    // A2. j_inc_bra != 0, j_inc_null == 0 (because starting from '[').
                    // A3. j_inc_bra == 0 && j_inc_null == 0, i.e. some other kind of non-array value.
                    if j_inc_bra + j_inc_null == 0 {
                        return ValidationResult::RepeatedFieldIsNotArray(ByteOffset(j), &field.name); // Case A3.
                    } else {
                        j += j_inc_bra; // Case A2 (for Case A1 this is a no-op, which is fine)
                        json_offset = json.offset(j);
                    }
                }

                let mut j_inc_val;
                loop {
                    j_inc_val = match field.type_ {
                        FieldType::STRING => {
                            micro_util::consume_string(&json_offset)
                        },
                        FieldType::BOOL => {
                            micro_util::consume_bool(&json_offset)
                        },
                        FieldType::FLOAT64 => {
                            micro_util::consume_float(&json_offset)
                        },
                        FieldType::INT64 => {
                            micro_util::consume_int64(&json_offset)
                        },
                        FieldType::DECIMAL_29_9 => {
                            micro_util::consume_decimal_29_9(&json_offset)
                        },
                        FieldType::DATE => {
                            // WARNING: doesn't fully validate day of month
                            micro_util::consume_date(&json_offset)
                        },
                        FieldType::TIME => {
                            micro_util::consume_time(&json_offset)
                        },
                        FieldType::DATETIME => {
                            // WARNING: doesn't fully validate day of month
                            micro_util::consume_datetime(&json_offset)
                        },
                        FieldType::TIMESTAMP => {
                            micro_util::consume_timestamp(&json_offset)
                        },
                        FieldType::BYTES => {
                            micro_util::consume_base64(&json_offset)
                        },
                        FieldType::ANY => {
                            micro_util::consume_json(&json_offset)
                        }
                        FieldType::STRUCT(_) => unreachable!("this case is handled spearately via the stack machinery in the if statement further up.")
                    };

                    if field.mode == FieldMode::REPEATED && j_inc_val > 0 {
                        j += j_inc_val;
                        j += micro_util::consume_punct::<b',', true>(&json.offset(j));
                        json_offset = json.offset(j);
                        continue; // this is the only case that we actually do the loop more than once
                    } 
                    break;
                }


                if field.mode == FieldMode::REPEATED {
                    // Cases:
                    // B1. j_inc_null != 0, then on first iteration j_inc_bra and j_inc_val can only be 0 (because they would be starting from an 'n').
                    //    so there was only one iteration.
                    // B2. j_inc_val == 0 (and j_inc_bra != 0 and j_inc_null == 0), because the array has been successfull consumed right up to the closing ']'
                    // B3. j_inc_val == 0 (and j_inc_bra == 0 and j_inc_null == 0) because the array couldn't be fully consumed because of a type mismatch.
                    let j_inc_bra = micro_util::consume_punct::<b']', false>(&json.offset(j));
                    if j_inc_bra + j_inc_null == 0 {
                        return ValidationResult::ArrayContentsInvalid(ByteOffset(j), &field.name); // Case B3.
                    } else {
                        j += j_inc_bra + j_inc_null; // Case B1 or B2
                    }
                } else if field.mode == FieldMode::REQUIRED && j_inc_null > 0 {
                    return ValidationResult::RequiredFieldIsNull(ByteOffset(j), &field.name);
                } else if j_inc_val + j_inc_null == 0 {
                    return ValidationResult::FieldValueInvalid(ByteOffset(j), &field.name);
                } else {
                    j += j_inc_val + j_inc_null;
                }
            }

            // consume comma - given we're assuming json is valid we don't check the return value
            j += micro_util::consume_punct::<b',', true>(&json.offset(j));
        }

        // consume closing bracket - given we're assuming valid json we don't check the return value
        j += micro_util::consume_punct::<b'}', false>(&json.offset(j));
                
        if let Some(absent_field) = stack_entry.schema.find(|field| field.mode == FieldMode::REQUIRED &&  !*seen_field_by_idx.get(field.idx).unwrap_or(&false) ) {
            return ValidationResult::RequiredFieldAbsent(ByteOffset(j), &absent_field.name);
        }

        // consume comma - given we're assuming json is valid we don't check the return value
        // this is needed for repeated structs, and for singleton structs that are followed by another member a level up
        j += micro_util::consume_punct::<b',', false>(&json.offset(j));

        if stack_entry.mode == FieldMode::REPEATED {
            let j_inc_close_array = micro_util::consume_punct::<b']', false>(&json.offset(j));
            if j_inc_close_array != 0 {
                // consume close array
                j += j_inc_close_array;
                stack.pop();
                // comma before the next member (if there is another member)
                j += micro_util::consume_punct::<b',', false>(&json.offset(j));
            } else {
                // reset the seen_field_by_idx for the repeated struct
                stack_entry.schema.fold((), |_, field| {
                    unsafe {
                        // SAFETY: seen_field_by_idx.len() set to max_field_idx+1 above.
                        //         It's the responsibility of the caller to enure that max_field_idx is set correctly.
                        hint::assert_unchecked(field.idx < seen_field_by_idx.len());
                    }
                    seen_field_by_idx[field.idx] = false
                });
            }
        } else {
            stack.pop();
        }
    }

    return ValidationResult::Valid;
}


#[cfg(test)]
mod tests {
    use super::*;

    /// convert a byte literal into a properly padded `u8p`
    macro_rules! u8p {
        ($x:literal) => {
            u8p::u8p::add_padding(&mut $x.into())
        }
    }

    const MFID: usize = 10; // max field idx

    fn make_base_schema() -> Vec<Field> {
        vec![
            Field {idx: 0, name: "str_field".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::STRING},
            Field {idx: 1, name: "date_field".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::DATE},
            Field {idx: 2, name: "datetime_field".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::DATETIME},
            Field {idx: 3, name: "time_field".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::TIME},
            Field {idx: 4, name: "timestamp_field".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::TIMESTAMP},
            Field {idx: 5, name: "bool_field".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::BOOL},
            Field {idx: 6, name: "int_field".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::INT64},
            Field {idx: 7, name: "float_field".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::FLOAT64},
            Field {idx: 8, name: "decimal_field".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::DECIMAL_29_9},
            Field {idx: 9, name: "bytes_field".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::BYTES},
            Field {idx: MFID, name: "any_field".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::ANY}
        ]
    }


    #[test]
    fn test_validate_basic() {
        let mut scratch = ValidateScratch::default();

        let schema = make_base_schema();
        let schema = AdaptivePrefixMap::create(
            schema.into_iter().map(|field| (field.name.clone(), field)).collect::<Vec<_>>());

        assert_eq!(validate(&schema, MFID, &u8p!(br#"{}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": null}"#), &mut scratch), ValidationResult::Valid{});
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": "this is a string"}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": "take1", "str_field": "take2"}"#), &mut scratch), ValidationResult::FieldDuplicated(ByteOffset(24), "str_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"random_field": null}"#), &mut scratch), ValidationResult::FieldUnrecognised(ByteOffset(2), "random_field"));

        let mut schema = make_base_schema();
        schema[0].mode = FieldMode::REQUIRED;
        let schema = AdaptivePrefixMap::create(
            schema.into_iter().map(|field| (field.name.clone(), field)).collect::<Vec<_>>());
        
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{}"#), &mut scratch), ValidationResult::RequiredFieldAbsent(ByteOffset(2), "str_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": "this is a string"}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": null}"#), &mut scratch), ValidationResult::RequiredFieldIsNull(ByteOffset(14), "str_field")); 

        let mut schema = make_base_schema();
        schema[0].mode = FieldMode::REPEATED;
        schema[5].mode = FieldMode::REQUIRED; // bool_field ..make it required to ensure we do find it after the any_field below...
        let schema = AdaptivePrefixMap::create(
            schema.into_iter().map(|field| (field.name.clone(), field)).collect::<Vec<_>>());
        
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{}"#), &mut scratch), ValidationResult::RequiredFieldAbsent(ByteOffset(2), "bool_field"));         
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": [], "bool_field": false}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": ["hello", "world"], "bool_field": false}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": "not an array!!!!", "bool_field": false}"#), &mut scratch), ValidationResult::RepeatedFieldIsNotArray(ByteOffset(14), "str_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": ["hello", 1234], "bool_field": false}"#), &mut scratch), ValidationResult::ArrayContentsInvalid(ByteOffset(24), "str_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": ["hello", null], "bool_field": false}"#), &mut scratch), ValidationResult::ArrayContentsInvalid(ByteOffset(24), "str_field")); 
    }


    #[test]
    fn test_validate_types(){
        let mut scratch = ValidateScratch::default();

        let schema = make_base_schema();
        let schema = AdaptivePrefixMap::create(
            schema.into_iter().map(|field| (field.name.clone(), field)).collect::<Vec<_>>());

        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": "this is a string"}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": "take1", "str_field": "take2"}"#), &mut scratch), ValidationResult::FieldDuplicated(ByteOffset(24), "str_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"str_field": 123}"#), &mut scratch), ValidationResult::FieldValueInvalid(ByteOffset(14), "str_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"date_field": "2025-03-01"}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"date_field": "2025-03-99}"#), &mut scratch), ValidationResult::FieldValueInvalid(ByteOffset(15), "date_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"datetime_field": "2025-03-01T13:05:00"}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"datetime_field": "2025-03-01T13:99:00}"#), &mut scratch), ValidationResult::FieldValueInvalid(ByteOffset(19), "datetime_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"time_field": "13:10:00.123"}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"time_field": "13:10:00!123}"#), &mut scratch), ValidationResult::FieldValueInvalid(ByteOffset(15), "time_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"timestamp_field": "2025-03-01T13:05:00 Z"}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"timestamp_field": "2025-03-01T13:05:00 X"#), &mut scratch), ValidationResult::FieldValueInvalid(ByteOffset(20), "timestamp_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"bool_field": false}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"bool_field": 42}"#), &mut scratch), ValidationResult::FieldValueInvalid(ByteOffset(15), "bool_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"int_field": 123456789}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"int_field": 12345678901234567801}"#), &mut scratch), ValidationResult::FieldValueInvalid(ByteOffset(14), "int_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"float_field": 123456789e+21}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"float_field": "shmoat"}"#), &mut scratch), ValidationResult::FieldValueInvalid(ByteOffset(16), "float_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"decimal_field": 123456789.123}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"decimal_field": 123456789.1234567890123}"#), &mut scratch), ValidationResult::FieldValueInvalid(ByteOffset(18), "decimal_field")); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"bytes_field": "xxxyy=="}"#), &mut scratch), ValidationResult::Valid{}); 
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"bytes_field": "xxxyy= ="}"#), &mut scratch), ValidationResult::FieldValueInvalid(ByteOffset(16), "bytes_field"));

        let mut schema = make_base_schema();
        schema[5].mode = FieldMode::REQUIRED; // bool_field ..make it required to ensure we do find it after the any_field below...
        let schema = AdaptivePrefixMap::create(
            schema.into_iter().map(|field| (field.name.clone(), field)).collect::<Vec<_>>());

        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"any_field": "string val"}"#), &mut scratch), ValidationResult::RequiredFieldAbsent(ByteOffset(27), "bool_field"));
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"any_field": "string val", "bool_field": false}"#), &mut scratch), ValidationResult::Valid{});
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"any_field": 123, "bool_field": false}"#), &mut scratch), ValidationResult::Valid{});
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"any_field": true, "bool_field": false}"#), &mut scratch), ValidationResult::Valid{});
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"any_field": [[true]], "bool_field": false}"#), &mut scratch), ValidationResult::Valid{});
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"any_field": [{"k":23}, [true]], "bool_field": false}"#), &mut scratch), ValidationResult::Valid{});
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"any_field": {"k":23}, "bool_field": false}"#), &mut scratch), ValidationResult::Valid{});
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"any_field": null, "bool_field": false}"#), &mut scratch), ValidationResult::Valid{});

    }

    #[test]
    fn test_validate_struct(){
        let mut scratch = ValidateScratch::default();

        let mut schema = make_base_schema();
        schema[5].mode = FieldMode::REQUIRED; // bool_field ..make it required to ensure we do find it after the any_field below...
        schema.pop();
        schema.pop();
        
        let sub_schema = vec![
            Field {idx: 9, name: "str_subfield".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::STRING},
            Field {idx: 10, name: "date_subfield".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::DATE},
        ];
        let sub_schema = AdaptivePrefixMap::create(
            sub_schema.into_iter().map(|field| (field.name.clone(), field)).collect::<Vec<_>>());
        schema[0].type_ = FieldType::STRUCT(Box::new(sub_schema));
        schema[0].name = "struct_field".to_string();
        let schema = AdaptivePrefixMap::create(
            schema.into_iter().map(|field| (field.name.clone(), field)).collect::<Vec<_>>());

        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"bool_field": true}"#), &mut scratch), ValidationResult::Valid);
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"struct_field": null, "bool_field": true}"#), &mut scratch), ValidationResult::Valid);
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"struct_field": {}, "bool_field": true}"#), &mut scratch), ValidationResult::Valid);
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"struct_field": {"random_field": 32}, "bool_field": true}"#), &mut scratch), ValidationResult::FieldUnrecognised(ByteOffset(19), "random_field"));
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"struct_field": {"str_subfield": "hi"}, "bool_field": true}"#), &mut scratch), ValidationResult::Valid);
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"struct_field": {"str_subfield": "hi", "date_subfield": "2024-04-12"}, "bool_field": true}"#), &mut scratch), ValidationResult::Valid);
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"struct_field": {"str_subfield": "hi", "date_subfield": "2024-04-12a"}, "bool_field": true}"#), &mut scratch), ValidationResult::FieldValueInvalid(ByteOffset(57), "date_subfield"));


        // Do the same thing, but now make the sub struct REPEATED
        let mut schema = make_base_schema();
        schema[5].mode = FieldMode::REQUIRED; // bool_field ..make it required to ensure we do find it after the any_field below...
        schema.pop();
        schema.pop();
        
        let sub_schema = vec![
            Field {idx: 9, name: "str_subfield".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::STRING},
            Field {idx: 10, name: "date_subfield".to_string(), mode: FieldMode::NULLABLE, type_: FieldType::DATE},
        ];
        let sub_schema = AdaptivePrefixMap::create(
            sub_schema.into_iter().map(|field| (field.name.clone(), field)).collect::<Vec<_>>());
        schema[0].type_ = FieldType::STRUCT(Box::new(sub_schema));
        schema[0].name = "struct_field".to_string();
        schema[0].mode = FieldMode::REPEATED; // THIS IS THE CHANGE
        let schema = AdaptivePrefixMap::create(
            schema.into_iter().map(|field| (field.name.clone(), field)).collect::<Vec<_>>());
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"bool_field": true}"#), &mut scratch), ValidationResult::Valid);
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"struct_field": null, "bool_field": true}"#), &mut scratch), ValidationResult::Valid);
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"struct_field": {}, "bool_field": true}"#), &mut scratch), ValidationResult::RepeatedFieldIsNotArray(ByteOffset(17), "struct_field"));
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"struct_field": [{"str_subfield": "hi"], "bool_field": true}"#), &mut scratch), ValidationResult::Valid);
        assert_eq!(validate(&schema, MFID, &u8p!(br#"{"struct_field": [], "bool_field": true}"#), &mut scratch), ValidationResult::Valid);


    }
}