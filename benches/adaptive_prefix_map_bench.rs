#![feature(portable_simd)]

use std::simd::prelude::*;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;


#[path="../src/micro_util.rs"]
mod micro_util;

#[path="../src/adaptive_prefix_map.rs"]
mod adaptive_prefix_map;
use adaptive_prefix_map::AdaptivePrefixMap;

#[path="../src/u8p.rs"]
mod u8p;

use u8p::u8pOwned;

fn criterion_benchmark(c: &mut Criterion) {

    let mut rng = rand::rng();


    let mut group = c.benchmark_group("adaptive_prefix_map");
    
    let kvs = vec![
        ("entity_def_id".to_string(), "val1".to_string()),
        ("image_id".to_string(), "val2".to_string()),
        ("location_type".to_string(), "val3".to_string()),
        ("permalink".to_string(), "val4".to_string()),
        ("uuid".to_string(), "val5".to_string()),
        ("value".to_string(), "val6".to_string()),
        ("1234567890123456789012345678901234".to_string(), "val7".to_string()), // with current implementation having a single key longer than 31 bytes forces use of the fallback
    ];
    let m = AdaptivePrefixMap::<String>::create(kvs);
    assert_eq!(m.mode(), "HashMapFallbackNoEscapes");
    assert_eq!(m.get_from_prefix(&u8p::u8p::add_padding(&mut b"entity_def_id\" continued".into())), Some(&"val1".to_string()));
    assert_eq!(m.get_from_prefix(&u8p::u8p::add_padding(&mut b"permalink\" and".into())), Some(&"val4".to_string()));
    let mut values: Vec<u8pOwned> = Vec::with_capacity(10000);
    for _ in 0..values.capacity() {
        values.push(if rng.random_bool(0.5) { u8pOwned::from(b"entity_def_id\" continued") } else {  u8pOwned::from(b"permalink\" and") });
    }
    let mut value_iter = values.iter().cycle(); 
    group.bench_function("HashMapFallbackNoEscapes", |b| b.iter(|| m.get_from_prefix( &value_iter.next().unwrap().as_borrowed())));
    

    let kvs = vec![
        ("entity_def_id".to_string(), "val1".to_string()),
        ("permalink".to_string(), "val4".to_string()),
    ];
    let m = AdaptivePrefixMap::<String>::create(kvs);
    assert_eq!(m.mode(), "ExhaustiveCompare2");
    assert_eq!(m.get_from_prefix(&u8p::u8p::add_padding(&mut b"entity_def_id\" continued".into())), Some(&"val1".to_string()));
    assert_eq!(m.get_from_prefix(&u8p::u8p::add_padding(&mut b"permalink\" and".into())), Some(&"val4".to_string()));
    let mut values: Vec<u8pOwned> = Vec::with_capacity(10000);
    for _ in 0..values.capacity() {
        values.push(if rng.random_bool(0.5) { u8pOwned::from(b"entity_def_id\" continued") } else {  u8pOwned::from(b"permalink\" and") });
    }
    let mut value_iter = values.iter().cycle(); 
    group.bench_function("ExhaustiveCompare2", |b| b.iter(|| m.get_from_prefix( &value_iter.next().unwrap().as_borrowed())));
  
    
    
    let kvs = vec![
        ("entity_type".to_string(), "val1".to_string()),
        ("entity_subtype".to_string(), "val2".to_string()),
        ("geometry".to_string(), "val3".to_string()),
        ("site_id".to_string(), "val4".to_string()),
        ("building_id".to_string(), "val5".to_string()),
        ("apartment_id".to_string(), "val6".to_string()),
        ("area_id".to_string(), "val7".to_string()),
    ];
    let m = AdaptivePrefixMap::<String>::create(kvs);
    assert_eq!(m.mode(), "SimdPerfectScanMap");
    assert_eq!(m.get_from_prefix(&u8p::u8p::add_padding(&mut b"entity_type\" continued".into())), Some(&"val1".to_string()));
    assert_eq!(m.get_from_prefix(&u8p::u8p::add_padding(&mut b"entity_subtype\" and".into())), Some(&"val2".to_string()));

    let mut values: Vec<u8pOwned> = Vec::with_capacity(10000);
    for _ in 0..values.capacity() {
        values.push(if rng.random_bool(0.5) { u8pOwned::from(b"entity_type\" continued") } else {  u8pOwned::from(b"entity_subtype\" and") });
    }
    let mut value_iter = values.iter().cycle(); 
    group.bench_function("SimdPerfectScanMap 1 lane x 2 chars", |b| b.iter(|| m.get_from_prefix( &value_iter.next().unwrap().as_borrowed())));
    


    let kvs = vec![
        ("aaaa".to_string(), "val1".to_string()),
        ("abaa".to_string(), "val2".to_string()),
        ("abca".to_string(), "val3".to_string()),
        ("abcd".to_string(), "val4".to_string()),
    ];
    let m = AdaptivePrefixMap::<String>::create(kvs);
    assert_eq!(m.mode(), "SimdPerfectScanMap");
    assert_eq!(m.get_from_prefix(&u8p::u8p::add_padding(&mut b"abaa\" continued".into())), Some(&"val2".to_string()));
    assert_eq!(m.get_from_prefix(&u8p::u8p::add_padding(&mut b"abcd\" and".into())), Some(&"val4".to_string()));
    let mut values: Vec<u8pOwned> = Vec::with_capacity(10000);
    for _ in 0..values.capacity() {
        values.push(if rng.random_bool(0.5) { u8pOwned::from(b"abaa\" continued") } else {  u8pOwned::from(b"abcd\" and") });
    }
    let mut value_iter = values.iter().cycle(); 
    group.bench_function("SimdPerfectScanMap 1 lane x 3 chars", |b| b.iter(|| m.get_from_prefix( &value_iter.next().unwrap().as_borrowed())));
    


}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

