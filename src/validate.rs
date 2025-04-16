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
    // TIMESTAMP
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
                    stack.push(StackEntry{schema: sub_schema.as_ref(), mode: field.mode, is_initialised: false});
                    continue 'stack_loop;
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
                        // unlike all the other consume logic, the date/time checks allow through strings that are very-nearly-but-not-quite compliant with our schema.
                        FieldType::DATE => {
                            const QUOTED_DATE_LOWER: &[u8; 12] = b"\"0000-00-00\"";
                            const QUOTED_DATE_UPPER: &[u8; 12] = b"\"9999/19/39\""; 
                            micro_util::consume_within_range(&json_offset, QUOTED_DATE_LOWER, QUOTED_DATE_UPPER)
                            // missing: check = ((json[6] < '1') | (json[7] <= '2')) &  (json[9] < '3') | (json[10] <= '1') ...but still allows invalid dates (do we need to deal with months, what about leap years?!)
                        },
                        FieldType::TIME => {
                            micro_util::consume_time(&json_offset)
                        },
                        FieldType::DATETIME => {
                            const QUOTED_DATETIME_LOWER: &[u8; 21] = b"\"0000-00-00T00:00:00\""; 
                            const QUOTED_DATETIME_UPPER: &[u8; 21] = b"\"9999/19/39T29:59:59\"";
                            micro_util::consume_within_range(&json_offset, QUOTED_DATETIME_LOWER, QUOTED_DATETIME_UPPER)
                            // missing: both kinds of checks above
                        },
                        FieldType::BYTES => {
                            micro_util::consume_base64(&json_offset)
                        }
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
