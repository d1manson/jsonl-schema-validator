#![feature(portable_simd)]

use std::simd::prelude::*;
use std::simd::u8x16;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::Rng;

#[path="../src/micro_util.rs"]
mod micro_util;

#[path="../src/u8p.rs"]
mod u8p;

use u8p::u8pOwned;


const QUOTED_DATE_LOWER: &[u8; 12] = b"\"0000-00-00\"";
const QUOTED_DATE_UPPER: &[u8; 12] = b"\"9999-19-39\""; 
const QUOTED_DATETIME_LOWER: &[u8; 21] = b"\"0000-00-00T00:00:00\""; 
const QUOTED_DATETIME_UPPER: &[u8; 21] = b"\"9999-19-39T29:59:59\""; 


fn is_quoted_date_ifs(json: &[u8]) -> bool {
    if json.len() < 12 {
        return false;
    }
    if !(json[0] == b'"' && json[5] == b'-' && json[8] == b'-' && json[11] == b'"') {
        return false;
    }
    for i in [1,2,3,4,6,7,9,10]{
        if !(json[i] >= b'0' && json[i] <= b'9'){
            return false;
        }
    }
    if !(json[6] <= b'1' && json[9] <= b'3') {
        return false;
    } 
    return true;
}

fn is_quoted_date_and(json: &[u8]) -> bool {
    if json.len() < 12 {
        return false;
    }

    let mut check = true; 
    check &=json[0] == b'"';

    check &=json[1] >= b'0';
    check &=json[1] <= b'9';

    check &=json[2] >= b'0';
    check &=json[2] <= b'9';

    check &=json[3] >= b'0';
    check &=json[3] <= b'9';

    check &=json[4] >= b'0';
    check &=json[4] <= b'9';

    check &=json[5] == b'-';

    check &=json[6] >= b'0';
    check &=json[6] <= b'1';

    check &=json[7] >= b'0';
    check &=json[7] <= b'9';

    check &=json[8] == b'-';

    check &=json[9] >= b'0';
    check &=json[9] <= b'3';

    check &=json[10] >= b'0';
    check &=json[10] <= b'9';

    check &=json[11] == b'"';

    return check;
}

#[repr(align(16))]
struct Align16<T>(T); // this is a 1-element tuple, use .0 to access the array itself

//#[inline(never)]
pub fn consume_quoted_date_simd(json: &[u8]) -> usize {
    // WARNING: this will allow through not-quite-valid dates like 2020-15-39 and 0000-00-00.
    if json.len() < 12 {
        return 0;
    }

    let mut buffer: Align16<[u8; 16]> = Align16([0; 16]);
    buffer.0[..12].copy_from_slice(&json[..12]);
    
    let min_vec = u8x16::from(*b"\"0000-00-00\"\0\0\0\0");
    let max_vec = u8x16::from(*b"\"9999-19-39\"\0\0\0\0");
    let data_vec = u8x16::from(buffer.0);
    let check  =  data_vec.simd_le(max_vec) & data_vec.simd_ge(min_vec);

    //println!("DEBUG INFO: json: {0:?} check: {check:?}, check.all: {1:?}", String::from_utf8(json.to_vec()).unwrap(), check.all());
    return if check.all() { 10 } else { 0 };
}

fn is_quoted_time_ifs(json: &[u8]) -> bool {
    if json.len() < 10 {
        return false;
    }
    if !(json[0] == b'"' && json[3] == b':' && json[6] == b':' && json[9] == b'"') {
        return false;
    }
    for i in [1,2,4,5,7,8]{
        if !(json[i] >= b'0' && json[i] <= b'9'){
            return false;
        }
    }
    if !(json[1] <= b'2' && json[4] <= b'5' && json[7] <= b'5') {
        return false;
    } 
    return true;
}


fn consume_quoted_date_generic(json: &u8p::u8p) -> usize {
    micro_util::consume_within_range(json, QUOTED_DATE_LOWER, QUOTED_DATE_UPPER)
}

fn consume_quoted_datetime_generic(json: &u8p::u8p) -> usize {
    micro_util::consume_within_range(json, QUOTED_DATETIME_LOWER, QUOTED_DATETIME_UPPER)
}

// #[inline(never)]
fn consume_colon(json: &[u8]) -> usize {
    let j = 0;
    
    // presumably there will be some consistent number of spaces either side of the colon, so the branch predictor
    // will work out which branch to follow here and then this will be super fast.
    if j+1 < json.len() && ((json[j] == b':') && (json[j+1] != b' ') )/*&& json[j+1] != b'\t'*/ {
        // colon with no spaces, followed by something
        return 1
    } else if j+2 < json.len() && ((json[j] == b':') && (json[j+1] == b' ' /*|| json[j+1] == b'\t'*/)  && (json[j+2] != b' ')) /*&& json[j+2] != b'\t' */{
        // colon followed by one space or tab and then something
        return 2
    } else if j+3 < json.len() && ((json[j] == b' ' /*|| json[j] == b'\t'*/) && (json[j+1] == b':') && (json[j+2] == b' ' /*|| json[j+2] == b'\t' */)  && (json[j+3] != b' ')) /*&& json[j+3] != b'\t'*/ {
        // colon with  one space or tab either side and then something
        return 3
    } 
    0 // TODO: implement general case
}


fn consume_bool(json: &[u8]) -> usize {
    let j = 0;

    if json.len() < 5 {
        return 0; // although true is only 4 bytes, there must be at least a b'}' at the end of the line following the word true, so we can require 5 bytes
    } else if &json[j..j+4] == b"true" {
        return 4;
    } else if &json[j..j+5] == b"false" {
        return 5;
    } else {
        return 0;
    }
}

//#[inline(never)]
fn consume_decimal_loop(json: &[u8]) -> usize {
    
    let mut j = 0;
    if j < json.len() && json[j] == b'-' {
        j+= 1;
       
    }

    let mut digits = 0;
    while j< json.len() && json[j] >= b'0' && json[j] <= b'9' {
        j += 1;
        digits += 1;
    }
    if digits > 29 {
        return  0;
    }

    if j < json.len() && (json[j] == b'e' ||  json[j] == b'E'){
        return 0;
    }

    if j < json.len() && json[j] == b'.' {
        j+= 1;
    }

    let mut digits = 0;
    while j< json.len() && json[j] >= b'0' && json[j] <= b'9' {
        j += 1;
        digits += 1;
    }

    if digits > 9 {
        return  0;
    }

    if j < json.len() && (json[j] == b'e' ||  json[j] == b'E'){
        return 0;
    }
    
    return  j;
}


//#[inline(never)]
fn consume_decimal_loop2(json: &[u8]) -> usize {
    const MAX_VALID_LEN: usize = 1 + 29 + 9 + 1 + 1; // sign + 29 digits + decimal point + 9 digits.
    const MAX_DP : usize = 1 + 29 + 1; // sign + 29 digits + decimal point

    if json.len() < 64 {
        return  0; // we require padding 
    }

    let mut dp_mask : u64 = 0;
    for j in 0..MAX_DP {
        dp_mask |= ((json[j] == b'.') as u64) << (63 - j);
    }

    let mut digits_mask : u64 = 0;
    for j in 0..MAX_VALID_LEN {
        digits_mask |= (((json[j] >= b'0') & (json[j] <= b'9')) as u64) << (63 - j);
    }
    digits_mask |= dp_mask;
    
    let has_sign = json[0] == b'-';
    let good_chars = (((has_sign as u64) << 63) | digits_mask).leading_ones() as usize;
    let dp_at = dp_mask.leading_zeros() as usize + 1;
        
    if (dp_at < 65) & (dp_at > 30 + has_sign as usize){
        return 0; // more than 29 digits to the left of the decimal point
    } else if (dp_at < 65) & (good_chars > dp_at + 9) {
        return 0; // more than 9 digits to the right of the decimal point
    } else if (dp_at == 65) & (good_chars > 29 + has_sign as usize) {
        return 0; // no dp, but more than 29 digits
    } if (json[good_chars] == b'e') | (json[good_chars] == b'E') { 
        return 0;  // the char following the good_chars is an e/E, which we don't accept here
    }

    return good_chars as usize;
}



//#[inline(never)]
fn consume_int_loop(json: &[u8]) -> usize {
    let mut j = 0;
    if json[j] == b'-' {
        j+= 1;
    }

    let mut digits = 0;
    while j< 22 && json[j] >= b'0' && json[j] <= b'9' {
        j += 1;
        digits += 1;
    }
    if json[j] == b'.' || json[j] == b'e' || json[j] == b'E' {
        return  0;
    }
    if digits == 19 {
        panic!("not implemented"); // the simd version does handle this properly
    }
    if digits > 19 {
        return  0;
    }
    return j;
}



//#[inline(never)]
fn consume_float_loop(json: &[u8]) -> usize {
    let mut j = 0;

    while (json[j] >= b'-' && json[j] <= b'E') || json[j] == b'+' || json[j] == b'e' {
        j += 1;
    }
    
    return j;
}


fn consume_string_loop(json: &[u8]) -> usize {
    let mut escaped = false;
    let mut j = 0;
    if j < json.len() && json[j] != b'"' {
        return 0;
    }
    j += 1;

    while j < json.len() && !(json[j] == b'"' && !escaped) {
        if json[j] == b'\\' {
            escaped = !escaped;
        }
        j += 1;
    }
    // doesn't properly check we closed the quote before the end, but whatever
    return j;
}

fn consume_brackets(json: &[u8]) -> usize {
    let mut j = 0;
    let mut depth = 0;
    if json[j] != b'{' {
        return 0;
    }
    j += 1;
    depth = 1;
    let mut in_quote = false;
    let mut in_escape = false;
    while depth > 0 && j < json.len(){
        if json[j] == b'{' && !in_quote {
            depth += 1;
        } else if json[j] == b'}' && !in_quote {
            depth -= 1;
        } else if json[j] == b'"' && !in_escape {
            in_quote = !in_quote;
        } else if json[j] == b'\\' {
            in_escape = !in_escape;
        }
        j += 1;
    }
    return j
} 

fn criterion_benchmark(c: &mut Criterion) {

    let mut rng = rand::rng();

    let mut group = c.benchmark_group("consume_float");
    // build 10k float/null values
    let mut values: Vec<u8pOwned> = Vec::with_capacity(10000);
    for _ in 0..values.capacity() {
        if rng.random_bool(0.1) {
            values.push(u8pOwned::from(b"null "));
        } else {
            let mut s = String::from("");
            if rng.random_bool(0.5) {
                s += "-";
            }
            s += &"1234567890123.456E+78901234567890123456"[0..rng.random_range(1..=14)];
            s += &" ";
            let s = u8pOwned::from(s);
            if micro_util::consume_float(&s.as_borrowed()) == 0 {
                panic!("test cases are suppsoed to be valid floats");
            }
            values.push(s);
        }
    }
    let mut value_iter = values.iter().cycle(); 
    group.bench_function("simd", |b| b.iter(|| micro_util::consume_float( black_box(&value_iter.next().unwrap().as_borrowed()))));
    group.bench_function("loop", |b| b.iter(|| consume_float_loop( black_box(&value_iter.next().unwrap().as_borrowed()))));   
    group.finish();

    let mut group = c.benchmark_group("consume_int64");
    // build 10k int/null values (we don't include valid json numbers that are invalid ints here)
    let mut values: Vec<u8pOwned> = Vec::with_capacity(10000);
    for _ in 0..values.capacity() {
        if rng.random_bool(0.1) {
            values.push(u8pOwned::from("null"));
        } else {
            let mut s = String::from("");
            if rng.random_bool(0.5) {
                s += "-";
            }
            s += &"12345678901234567890"[0..rng.random_range(1..=18)];   
            s += &" ";         
            let s = u8pOwned::from(s);
            if micro_util::consume_int64(&s.as_borrowed()) == 0 {
                panic!("test cases are suppsoed to be valid ints");
            }
            values.push(s);
        }
    }
    let mut value_iter = values.iter().cycle(); 
    group.bench_function("simd", |b| b.iter(|| micro_util::consume_int64( black_box(&value_iter.next().unwrap().as_borrowed()))));
    group.bench_function("loop", |b| b.iter(|| consume_int_loop( black_box(&value_iter.next().unwrap().as_borrowed()))));   
    group.finish();


    let mut group = c.benchmark_group("consume_decimal");
    // build 10k numeric/null values (we don't include valid json numbers that are invalid numerics here)
    let mut values: Vec<u8pOwned> = Vec::with_capacity(10000);
    for _ in 0..values.capacity() {
        if rng.random_bool(0.1) {
            values.push(u8pOwned::from(b"null"));
        } else {
            let mut s = String::from("");
            if rng.random_bool(0.5) {
                s += "-";
            }
            s += &"012345678901234567890123456789"[0..rng.random_range(4..=29)];
            if rng.random_bool(0.5) {
                s += &".01234567890"[0..rng.random_range(2..=10)]
            }
            s += &" ";
            let s = u8pOwned::from(s);
            if micro_util::consume_decimal_29_9(&s.as_borrowed()) == 0 {
                panic!("test cases are suppsoed to be valid decimals");
            }
            values.push(s);
        }
    }
    let mut value_iter = values.iter().cycle(); 
    group.bench_function("simd", |b| b.iter(|| micro_util::consume_decimal_29_9( black_box(&value_iter.next().unwrap().as_borrowed()))));
    //group.bench_function("loop2", |b| b.iter(|| consume_decimal_loop2( black_box(&value_iter.next().unwrap().as_borrowed()))));
    group.bench_function("loop", |b| b.iter(|| consume_decimal_loop( black_box(&value_iter.next().unwrap().as_borrowed()))));
    group.finish();


    let mut group = c.benchmark_group("consume_within_range \"2025-01-01\"");
    let value = b"\"2025-01-01\"";
    group.bench_function("many ifs", |b| b.iter(|| is_quoted_date_ifs( black_box(value))));
    group.bench_function("bitwise ands", |b| b.iter(|| is_quoted_date_and( black_box(value))));
    group.bench_function("simd", |b| b.iter(|| consume_quoted_date_simd( black_box(value))));
    let value = u8pOwned::from(value);
    group.bench_function("simd generic", |b| b.iter(|| consume_quoted_date_generic( black_box(&value.as_borrowed()))));
    group.finish();

    
    let mut group = c.benchmark_group("consume_within_range \"2025-01-01T23:01:00\"");
    let value = u8pOwned::from(b"\"2025-01-01T23:01:00\"");
    group.bench_function("simd generic", |b| b.iter(|| consume_quoted_datetime_generic( black_box(&value.as_borrowed()))));
    group.finish();


    let mut group = c.benchmark_group("consume_colon");    
    let test_cases :Vec<&str>  = vec!("", ":x", ": x"," : x", "} ", " }  }");
    for test_case in test_cases {
        group.bench_function(format!("3 if statements \"{test_case}\""), |b| b.iter(|| consume_colon( black_box(test_case.as_bytes()))));
        let mut v = test_case.as_bytes().to_vec();
        let val = u8p::u8p::add_padding(&mut v);
        group.bench_function(format!("loop \"{test_case}\""), |b| b.iter(|| micro_util::consume_punct::<b':', true>( black_box(&val))));

    }
    group.finish();


    let mut values: Vec<u8pOwned> = Vec::with_capacity(10000);
    let mut rng = rand::rng();
    for _ in 0..values.capacity() {
        values.push(if rng.random_bool(0.5) { u8pOwned::from(b"true   ") } else { u8pOwned::from(b"false   ") });
    }
    let mut value_iter = values.iter().cycle(); 

    let mut group = c.benchmark_group("consume_bool");
    group.bench_function("basic: if statements rand", |b| b.iter(|| consume_bool( &value_iter.next().unwrap().as_borrowed())));
    group.bench_function("opt: if statements rand", |b| b.iter(|| micro_util::consume_bool( &value_iter.next().unwrap().as_borrowed())));
    
    group.finish();



    let mut values: Vec<u8pOwned> = Vec::with_capacity(10000);
    for _ in 0..values.capacity() {
        values.push(if rng.random_bool(0.5) { u8pOwned::from("\"something\"      ") } else { u8pOwned::from(b"\"at\\\"her\\\\\"        ")});
    }
    let mut value_iter = values.iter().cycle(); 

    let mut group = c.benchmark_group("consume_string");
    group.bench_function("loop", |b| b.iter(|| consume_string_loop( &value_iter.next().unwrap().as_borrowed())));
    group.bench_function("simd", |b| b.iter(|| micro_util::consume_string( &value_iter.next().unwrap().as_borrowed())));
     
    group.finish();


    let mut group = c.benchmark_group("consume_json");
    let mut values: Vec<u8pOwned> = Vec::with_capacity(10000);
    for _ in 0..values.capacity() {
        values.push(if rng.random_bool(0.5) { u8pOwned::from(b"{\"xx\":1,\"y\":{}}     ") } else {  u8pOwned::from(b"{\"y\":\"\\\"\",\"x\":3}     ") });
    }
    let mut value_iter = values.iter().cycle(); 
    group.bench_function("loop", |b| b.iter(|| consume_brackets( &value_iter.next().unwrap().as_borrowed())));
    group.bench_function("simd", |b| b.iter(|| micro_util::consume_json( &value_iter.next().unwrap().as_borrowed())));
    group.finish();


    
    let mut group = c.benchmark_group("is_odd_contiguous");
    let mut values: Vec<u64> = Vec::with_capacity(10000);
    for _ in 0..values.capacity() {
        values.push(if rng.random_bool(1.0) { 8 } else { 12 });
    }
    let mut value_iter = values.iter().cycle(); 
    group.bench_function("bitwise<16>", |b| b.iter(|| micro_util::is_odd_contiguous::<16>( black_box(*value_iter.next().unwrap()), false)));
    group.bench_function("bitwise<64> carry:false", |b| b.iter(|| micro_util::is_odd_contiguous::<64>(black_box(*value_iter.next().unwrap()), false)));
    group.bench_function("bitwise<64> carry:blackbox", |b| b.iter(|| micro_util::is_odd_contiguous::<64>(black_box(*value_iter.next().unwrap()), black_box(false))));
    group.finish();

    
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

