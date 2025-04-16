#![feature(portable_simd)]

use std::fs::File;
use std::io::{self, BufRead};
use std::process;
use clap::Parser;
use serde_json;
use serde::Deserialize;
use std::time::Instant;
use human_format::{Scales, Formatter};
use crossbeam_channel::{bounded as cb_bchannel, Receiver};



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
    #[arg(short='x', long, action=clap::ArgAction::SetTrue)]
    exit_on_first_error: bool,

    // main thread reads from file, worker threads do validation
    #[arg(short='t', long, default_value="4")]
    num_worker_threads: usize
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
        "BYTES" => FieldType::BYTES,
        "BASE64" => FieldType::BYTES,
        
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

    // once we've built the schema, we can now spawn some worker threads that get immutable borrows of it (we could actually copy it, but this works too)
    let pool_capacity  = args.num_worker_threads * 3;


    std::thread::scope(|thread_scope| {

    let (main_to_worker, worker_from_main) = cb_bchannel(pool_capacity);
    let (worker_to_main, main_from_worker) = cb_bchannel(pool_capacity); // this could switch to mpsc from std, but using crossbeam for consistency
    
    // We jump through a few hoops here to not reallocate on the heap once the loop has warmped up.
    // Firstly, each thread creates a scratch instance which will grow in size to begin with, but then remain constant.
    // Secondly, we only have a fixed pool of Strings, which we use to read from the file, send to the worker threads, and then recycle back for subsequent lines.
    // This may require reallocating for the first few times, until we've seen some really long lines, but then not after that. Note that this is handy because
    // we add zero padding to lines, so it's nice if the capacity is already there, rather than needing to do a copy in order to add padding.

    for _ in 0..args.num_worker_threads {
        let worker_from_main : Receiver<(usize, String)> = worker_from_main.clone();
        let worker_to_main = worker_to_main.clone();
        let schema = &schema;
        thread_scope.spawn(move || {
            let mut scratch = ValidateScratch::default();

            for (line_num, line) in worker_from_main {
                if line_num == 0 {
                    break;
                }
                let mut line_bytes = line.into_bytes();
                let line_u8p = u8p::u8p::add_padding(&mut line_bytes);

                let result = validate(schema, next_field_idx-1, &line_u8p, &mut scratch);
                let is_valid = result == ValidationResult::Valid;
                if !is_valid {
                    // WARNING: when there's more than one thread, we don't enforce any ordering of these logs
                    eprintln!("{line_num}: {result:?}");
                }
                
                line_bytes.clear();
                let recyclable_string = unsafe {
                    // SAFETY: the line_bytes is empty, so it can't possibly be invalid
                    String::from_utf8_unchecked(line_bytes)
                };
                worker_to_main.send((is_valid, recyclable_string)).unwrap();
            }
        });
    }
    drop(worker_to_main); // only the threads should have a copy of this now, so they can control when the worker_to_main is completed.

    let file = File::open(args.jsonl_file.clone()).unwrap_or_else(|error| panic!("Problem opening the jsonl file {0:?}: {error:?}", args.jsonl_file));
    let mut reader = io::BufReader::new(file);
    let mut byte_count: usize = 0;
    let mut line_count: usize = 0;
    let mut err_count: usize = 0;

    let start_time = Instant::now();
    
    // fill channel to capacity
    for _ in 0..pool_capacity {
        let mut line = "".to_string();
        let bytes_read = reader.read_line(&mut line).unwrap_or_else(|err| panic!("Reading file failed on line {line_count}: {err}"));
        if bytes_read == 0 {
            break;
        }
        line_count += 1;
        byte_count += line.as_bytes().len() + 1; // assume newline is 1 byte
        main_to_worker.send((line_count, line)).unwrap();
    }

    // Main loop: keep reading lines from the file and receiving the results from the workers until both are done 
    for (is_valid, recyclable_string) in main_from_worker {
        // deal with error
        if !is_valid && args.exit_on_first_error {
            process::exit(1); // WARNING: when there's more than one thread, we don't bother to enforce the ordering of this
        }
        err_count += !is_valid as usize;
        
        // recyle the string (i.e. the heap allocation), for the next line, which goes back into the channel
        let mut line = recyclable_string;
        let bytes_read = reader.read_line(&mut line).unwrap_or_else(|err| panic!("Reading file failed on line {line_count}: {err}"));
        if bytes_read > 0 {
            line_count += 1;
            byte_count += line.as_bytes().len() + 1; // assume newline is 1 byte
            main_to_worker.send((line_count, line)).unwrap();
        } else {
            // sending line_num:0 is the signal to the worker thread to shut down (see the if-break statement in the worker loop).
            // when all worker_to_main instances are dropped by the closed threads (and all final messages have been processed here), this loop
            // itself will terminate. It's simpler to use 0 as a speical message rather than trying to drop the main_to_worker inside this loop.
            main_to_worker.send((0, line)).unwrap();
        } 
    }
    let duration = start_time.elapsed();

    // report to the user the final outcome
    println!("Read {0} / {1} with {err_count} errors in {2:?}. {3} | {4}", 
        Formatter::new().with_separator("").with_units(" lines").format(line_count as f64),
        Formatter::new().with_separator("").with_scales(Scales::Binary()).with_units("B").format(byte_count as f64),
        duration,
        Formatter::new().with_separator("").with_units(" lines/s").format(line_count as f64 / duration.as_secs_f64() as f64), 
        Formatter::new().with_separator("").with_units("B/s").format(byte_count as f64 / duration.as_secs_f64() as f64)
    );
    if err_count > 0 {
        process::exit(1);
    }


    });

    
}
