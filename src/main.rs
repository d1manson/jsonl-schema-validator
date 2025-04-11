#![feature(portable_simd)]

use std::fs::File;
use std::io::{self, BufRead};
use std::process;
use clap::Parser;
use serde_json;
use serde::Deserialize;
use std::time::Instant;

pub(crate) mod u8p;
pub(crate) mod micro_util;
pub(crate) mod adaptive_prefix_map;
pub(crate) mod validate;
use adaptive_prefix_map::AdaptivePrefixMap;
use validate::{validate, Field, FieldMode, FieldType, ValidateScratch, ValidationResult};




#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The jsonl file to validate
    #[arg(short='f', long)]
    jsonl_file: String,

    /// The file defining the schema
    #[arg(short='s', long)]
    schema_file: String,

    // either print all errors, or just the first
    #[arg(short='x', long, action = clap::ArgAction::SetTrue)]
    exit_on_first_error: bool,

}



#[derive(Deserialize)]
struct SerdeField {
    name: String,
    r#type: String,
    mode: Option<String>,
    fields: Option<Vec<SerdeField>>
}


fn field_from_json(field_json: &SerdeField, next_field_idx: &mut usize) -> Field {
    let idx = *next_field_idx;
    *next_field_idx += 1;

    let mode = match field_json.mode.clone().unwrap_or("NULLABLE".to_string()).as_str() {
        "REQUIRED" => FieldMode::REQUIRED,
        "REPEATED" => FieldMode::REPEATED,
        "NULLABLE" => FieldMode::NULLABLE,
        m => panic!("uncregonised mode: {m}")
    };

    let type_ = match field_json.r#type.as_str() {
        "STRUCT" => {
            let sub_schema = field_json.fields.as_ref()
                .unwrap_or_else(|| panic!("\"type\": \"STRUCT\" requires \"fields\": [...] sub schema."))
                .into_iter()
                .map(|field| (field.name.clone(), field_from_json(&field, next_field_idx)))
                .collect::<Vec<_>>();
            FieldType::STRUCT(Box::new(AdaptivePrefixMap::create(sub_schema)))
        },
        "DATE" => FieldType::DATE,
        "DATETIME" => FieldType::DATETIME,
        "TIME" => FieldType::TIME,
        
        "BOOL" => FieldType::BOOL,
        "BOOLEAN" => FieldType::BOOL,
        "INT64" => FieldType::INT64,
        "INT" => FieldType::INT64,
        "FLOAT64" => FieldType::FLOAT64,
        "FLOAT" => FieldType::FLOAT64,

        "DECIMAL_29_9" => FieldType::DECIMAL_29_9,
        "DECIMAL" => FieldType::DECIMAL_29_9,
        "NUMERIC" => FieldType::DECIMAL_29_9,

        "STRING" => FieldType::STRING,
        "ANY" => FieldType::ANY,
        "JSON" => FieldType::ANY,
        t => panic!("Unrecognised type: {t}")
    };

    return Field { idx, name: field_json.name.clone(), mode, type_ };
}

fn main() {

    let args = Args::parse();

    let schema_file =  File::open(args.schema_file.clone()).unwrap_or_else(|error| panic!("Problem opening the schema file {0:?}: {error:?}", args.schema_file));
    let schema_file_reader = io::BufReader::new(schema_file);

    let schema_json : Vec<SerdeField> = serde_json::from_reader(schema_file_reader).unwrap_or_else(|error| panic!("Failed to parse the schema file: {error}"));
    let mut next_field_idx = 0;
    let schema = schema_json
                .into_iter()
                .map(|field| (field.name.clone(), field_from_json(&field, &mut next_field_idx)))
                .collect::<Vec<_>>();
    let schema = AdaptivePrefixMap::create(schema);

    let file = File::open(args.jsonl_file.clone()).unwrap_or_else(|error| panic!("Problem opening the jsonl file {0:?}: {error:?}", args.jsonl_file));
    let mut reader = io::BufReader::new(file);

    let mut scratch = ValidateScratch::default();

    // we jump through a few hoops here to keep a single buffer on the heap representing a line of json. It starts as a belonging to a String,
    // which is populated by read_line. Ownership is then moved to a Vec<u8>, which gets padding added (without any reallocation/copying if the
    // buffer's capacity was increased sufficiently on pervious iterations), and then returned back to being the string, with zero length, ready
    // for use by the next_line again on the next iteration.
    let mut line = String::default();
    let mut line_n = 0;
    let mut err_count = 0;

    let start_time = Instant::now();
    while let Ok(bytes_read) = reader.read_line(&mut line) {
        if bytes_read == 0 {
            break;
        }
        line_n += 1;
        let mut line_bytes = line.into_bytes();
        let line_u8p = u8p::u8p::add_padding(&mut line_bytes);
        //print!("{line_u8p:?}");
        
        let result = validate(&schema, next_field_idx-1, &line_u8p, &mut scratch);
        
        if result != ValidationResult::Valid {
            println!("{line_n}: {result:?}");
            if args.exit_on_first_error {
                process::exit(1);
            }
            err_count += 1;
        }
        line_bytes.clear();
        unsafe {
            // SAFETY: the line_bytes is empty, so it can't possibly be invalid
            line = String::from_utf8_unchecked(line_bytes);
        }   
    }

    println!("Read {line_n} lines with {err_count} errors, in {0:?} ({1:?} per line)", start_time.elapsed(), start_time.elapsed()/line_n);
    if err_count > 0 {
        process::exit(1);
    }
}
