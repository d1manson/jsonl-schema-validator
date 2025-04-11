use std::simd::num::SimdInt;
use std::{hint, usize};
use std::fmt::{self, Debug};
use std::simd::cmp::SimdPartialEq;
use std::simd::{Simd, SupportedLaneCount, LaneCount};
use core::array;
use rustc_hash::FxHashMap;

use crate::micro_util;
use crate::u8p::{u8p, DEFAULT_LANE_SIZE};


trait PrefixMap<T> {
    
    /// If the keys satisfy the requirements for this kind of map it'll return a new instance, otherwise it returns the `key_vals` back
    fn try_create(key_vals: Vec<(String, T)>) -> Result<Self, Vec<(String, T)>> where Self: Sized;

    /// `data` starts on the byte after the opening double quote of a key within a query string. The function finds the terminating 
    /// double quote in the query string, and looks up the key (the bit between the double quotes) within the map, returning the value
    /// if the key is found.
    /// 
    /// Note that implementations may avoid explicitly searching for the terminating double quote if they are able to quickly identify the
    /// key directly, but the logic must at some point verify that the double quote is there as expected.
    fn get_from_prefix(&self, data: &u8p) -> Option<&T>;

    /// It's difficult to include an .iter() method in the trait if we want to permit implementations total control over
    /// the underlying storage logic, while still being efficient. So instead we offer a fold (aka reduce) function as that can have a
    /// standard signature.
    fn fold<Acc, F: FnMut(Acc, &T) -> Acc>(&self, init: Acc, cb: F) -> Acc;

    /// It's difficult to include an .iter() method in the trait if we want to permit implementations total control over
    /// the underlying storage logic, while still being efficient. So instead we offer a find function as that can have a
    /// standard signature. Returns the first value which matches the predicate function.
    fn find<F: Fn(&T) -> bool>(&self, cb: F) -> Option<&T>;
}

/// Compares the first `LANE_SIZE` bytes from the query string against N keys. If any key matches exactly, it returns the associated
/// value.
/// 
/// Requires that there are exactly `N` keys, and all keys are `LANE_SIZE-1` bytes or less (the `-1` allows for the terminating 
/// double quote to be included within the SIMD lane).
struct ExhaustiveCompare<T, const N: usize, const LANE_SIZE: usize> 
where LaneCount<LANE_SIZE>: SupportedLaneCount
{
    key_vals: [(Simd<u8, LANE_SIZE>, T); N]
}

impl<T, const N: usize,  const LANE_SIZE: usize> PrefixMap<T> for ExhaustiveCompare<T, N, LANE_SIZE> 
where LaneCount<LANE_SIZE>: SupportedLaneCount{

    fn try_create(key_vals: Vec<(String, T)>) -> Result<Self, Vec<(String, T)>> {
        if key_vals.len() != N {
            return Err(key_vals);
        }
        for idx in 0..N {
            if key_vals[idx].0.as_bytes().len() > LANE_SIZE-1 {
                return Err(key_vals);
            }
        }

        let as_arr: Result<[(Simd<u8, LANE_SIZE>, T); N], _> = key_vals.into_iter().map(|(key, val)| {
            let key= key.as_bytes();
            let mut key_match = Simd::<u8, LANE_SIZE>::splat(0);
            key_match[0..key.len()].copy_from_slice(key);
            key_match[key.len()] = b'"';
            (key_match, val)
        }).collect::<Vec<_>>().try_into();

        let as_arr = unsafe {
            // SAFETY: we already checked key_vals.len() != N at the start, so this must be fine
             as_arr.unwrap_unchecked()
        };

        Ok(ExhaustiveCompare::<T, N, LANE_SIZE>{key_vals: as_arr})
    }

    /// This is branchless when compiled (at least for `N=1..=2`)
    fn get_from_prefix(&self, data: &u8p) -> Option<&T>{
        let data = data.initial_lane::<LANE_SIZE, 0>();
        let matched : [bool; N] = array::from_fn(|idx| {
            (data.simd_eq(self.key_vals[idx].0) | self.key_vals[idx].0.simd_eq(Simd::<u8, LANE_SIZE>::splat(0))).all()
        });
        let mut found = false;
        let mut found_idx = 0;
        for idx in 0..N {
            found |= matched[idx];
            if matched[idx] {
                 found_idx = idx 
            }
        }
        if found { Some(&self.key_vals[found_idx].1) } else { None } 
    }


    fn fold<Acc, F: FnMut(Acc, &T) -> Acc>(&self, init: Acc, mut cb: F) -> Acc {
        self.key_vals.iter().fold(init, |acc, (_, v)| cb(acc, v))
    }


    fn find<F: Fn(&T) -> bool>(&self, cb: F) -> Option<&T> {
        if let Some((_, v)) = self.key_vals.iter().find(|(_,v)| cb(v)) {
            return Some(&v);
        } else {
            return None;
        }
    }
}


fn roughly_log_2(x: usize) -> usize {
    (usize::BITS - x.leading_zeros()) as usize
}

/// If you have two simd vectors of the same size, this will compare them from element 0..valid_n, ignoring any mismatches from valid_n onwards.
fn simd_eq_fast<const LANE_SIZE: usize>(a: Simd<u8, LANE_SIZE>, b: Simd<u8, LANE_SIZE>, valid_n: usize) -> bool
where LaneCount<LANE_SIZE>: SupportedLaneCount {
    // It might seem a bit odd to use actual numbers and reduce_max rather than using bools, but it's faster this way at least on Arm Neon (Macs).
    // actually it seems that it might depend on LANE_SIZE or other conditions, so don't assume this is faster.
    let not_matched: [i8; LANE_SIZE] = array::from_fn(|i| (LANE_SIZE - i) as i8); 
    let mut not_matched = Simd::<i8, LANE_SIZE>::from(not_matched);

    not_matched &= a.simd_ne(b).to_int();
    let first_mismatch = LANE_SIZE - not_matched.reduce_max() as usize;
    return first_mismatch >= valid_n;
}

/// This gave birth to a standalone repo [here](https://github.com/d1manson/rust-simd-psmap), though that version has diverged somewhat
/// from this as it doesn't use the u8p thing at all, and that version needs the full key when querying, whereas here we just know where
/// the key starts not where it ends. That said, that repo's readme has a decent introduction to this. Also, to keep things simple, here
/// we only support up to LANE_SIZE_SCAN number of keys.
/// 
/// There are no branches in the compiled getter code, excecpt for the loop, which is hopefully easy enough for the branch predictor to 
/// understand as it always does the same number of iterations for a given instance of the struct (there's no early exit condition).
/// 
/// Requirements:
///  - Crucially, `MAX_N_CHARS` is the hard limit on how many chars it will scan, so it must be possible to disambiguate with that number of fewer.
///    Also (and this is unlikely to ever be a problem) it must be possible to disambiguate between keys when trunctating keys to the first 
///    `u8p::PADDING_SIZE` bytes (this plays nicely with `u8p`'s padding guarantee, saving some cycles).
///  -  All keys need to have length of at most `LANE_SIZE_VERIFY-1` to make the final verify step fast. We could use a vec to store the
///     full keys for the verify step, but for extra speed we also use SIMD here, but capped as this length.
struct SimdPerfectScanMap<T, const MAX_N_CHARS: usize, const LANE_SIZE_SCAN: usize, const LANE_SIZE_VERIFY: usize> 
where LaneCount<LANE_SIZE_SCAN>: SupportedLaneCount,
      LaneCount<LANE_SIZE_VERIFY>: SupportedLaneCount {
    key_vals: Vec<(Simd<u8, LANE_SIZE_VERIFY>, usize, T)>, // (key zero-padded within Simd, key true length including terminating quote, value)
    n_chars: usize,  
    // we allocate the below as inline arrays, but we only need n_chars elements (always <= MAX_N_CHARS)
    char_positions: [usize; MAX_N_CHARS],
    indexes: [Simd<u8, LANE_SIZE_SCAN>; MAX_N_CHARS]   
}

impl<T, const MAX_N_CHARS: usize,  const LANE_SIZE_SCAN: usize, const LANE_SIZE_VERIFY: usize> PrefixMap<T> 
for SimdPerfectScanMap<T, MAX_N_CHARS, LANE_SIZE_SCAN, LANE_SIZE_VERIFY> 
where LaneCount<LANE_SIZE_SCAN>: SupportedLaneCount,
      LaneCount<LANE_SIZE_VERIFY>: SupportedLaneCount {

    fn try_create(key_vals: Vec<(String, T)>) -> Result<Self, Vec<(String, T)>> {
        if key_vals.len() == 0 || key_vals.len() > LANE_SIZE_SCAN {
            return Err(key_vals);
        }

        let max_key_len = key_vals.iter().map(|(k, _)| k.as_bytes().len()).max().unwrap();
        if max_key_len > LANE_SIZE_VERIFY - 1 {
            return Err(key_vals);
        }

        let mut char_positions = vec![0; 0];
        let max_char_position = max_key_len.min(u8p::PADDING_SIZE - 1);
        // Yes, there are a lot of nested loops here, but MAX_N_CHARS and max_char_position are capped fairly low.
        // If needed there are definitely some straightforward ways to reduce the complexity here, such as by storing the
        // selected characters themselves (as we end up doing in the `indexes` later) and sort after each new char so that
        // duplicates appear next to one another. Then when adding a new char you just need to loop over existing block of 
        // duplicates rather than all other keys, and count how many are still dups as you go. But in reality this is taking
        // less than 1ms at startup so it's not worth over complicating.
        let mut solved = false;
        for _ in 1..=MAX_N_CHARS {
            let mut position_score = vec![0; max_char_position];
            for new_char_idx in 0..max_char_position {
                if char_positions.contains(&new_char_idx){
                    position_score[new_char_idx] = usize::MAX;
                    continue;
                }
                char_positions.push(new_char_idx); // temporarily add it to calculate a score
            
                for (k_self, _) in &key_vals {
                    // each key contributes to the score for new_char_idx...
                    let k_self = k_self.as_bytes();
                    let mut tests_match_keys = vec![true; key_vals.len()];
                    for &char_idx_sub in char_positions.iter() {
                        let char_self = if char_idx_sub == k_self.len() { b'"' } else { *k_self.get(char_idx_sub).unwrap_or(&0) };
                        for (idx, (k_other, _)) in key_vals.iter().enumerate() {
                            let k_other = k_other.as_bytes();
                            let char_other = if char_idx_sub == k_other.len() { b'"' } else { *k_other.get(char_idx_sub).unwrap_or(&0) };
                            tests_match_keys[idx] &= (char_self == char_other) | (char_other == 0);
                        }
                    }
                    let tests_match_n_other_keys: usize = tests_match_keys.iter().map(|&b| b as usize).sum::<usize>() - 1;
                    position_score[new_char_idx] += roughly_log_2(tests_match_n_other_keys); 
                }

                char_positions.pop(); // as promised, adding the new_char was only temporary
            }
            let best_idx = position_score.iter().enumerate().min_by_key(|(_, s)| *s).unwrap().0;
            char_positions.push(best_idx);
            if position_score[best_idx] == 0 {
                solved = true;
                break;
            }
        }

        if !solved {
            return Err(key_vals);
        }

        let n_chars = char_positions.len();
        let mut indexes = [Simd::<u8, LANE_SIZE_SCAN>::splat(0); MAX_N_CHARS];     
        for (scan_idx, char_idx) in char_positions.iter().enumerate() {
            let mut v = Simd::<u8, LANE_SIZE_SCAN>::splat(0);
            for (idx, (k, _)) in key_vals.iter().enumerate() {
                let k = k.as_bytes();
                v[idx] = if *char_idx == k.len() { b'"' } else { *k.get(*char_idx).unwrap_or(&0) };
            }
            indexes[scan_idx] = v;
        }

        let mut ret = SimdPerfectScanMap::<T, MAX_N_CHARS, LANE_SIZE_SCAN, LANE_SIZE_VERIFY>{
            n_chars,
            char_positions: [0; MAX_N_CHARS],
            indexes,
            key_vals: key_vals.into_iter().map(|(key,v)| {
                let key = key.as_bytes();
                let mut key_match = Simd::<u8, LANE_SIZE_VERIFY>::splat(0);
                key_match[0..key.len()].copy_from_slice(key);
                key_match[key.len()] = b'"';
                (key_match, key.len()+1, v)
            }).collect()
        };

        ret.char_positions[0..ret.n_chars].copy_from_slice( &char_positions);
        return Ok(ret);

    }

    fn get_from_prefix(&self, data: &u8p) -> Option<&T>{
        unsafe {
            // SAFETY: designed that way in `try_create` method, which is the only way to construct this struct
            hint::assert_unchecked(self.n_chars >= 1);
        }

        let matched: [i8; LANE_SIZE_SCAN] = array::from_fn(|i| (LANE_SIZE_SCAN - i) as i8); 
        let mut matched = Simd::<i8, LANE_SIZE_SCAN>::from(matched);
        for scan_idx in 0..self.n_chars {
            unsafe {
                hint::assert_unchecked(scan_idx < MAX_N_CHARS);
            }
            let char_idx = self.char_positions[scan_idx];
            unsafe {
                // SAFETY: designed that way in `try_create` method, which is the only way to construct this struct
                hint::assert_unchecked(char_idx < u8p::PADDING_SIZE);
            }
            let data_c = data.raw_u8s()[char_idx];
            let index = self.indexes[scan_idx];
            matched &= (index.simd_eq(Simd::<u8, LANE_SIZE_SCAN>::splat(data_c)) | index.simd_eq(Simd::<u8, LANE_SIZE_SCAN>::splat(0))).to_int();
        }
        let matched_idx = LANE_SIZE_SCAN - matched.reduce_max() as usize; // amazingly, using reduce_max(), having started with [16, 15, ..., 1, 0] is faster than using a mask and .first_set()
        let matched_idx = matched_idx.min(self.key_vals.len() - 1);
        unsafe {
            // SAFETY: the compiler doesn't seem to deduce this assertion from the above .min(...) for some reason
            hint::assert_unchecked(matched_idx < self.key_vals.len());
        }
        let found = &self.key_vals[matched_idx];
        if simd_eq_fast(found.0, data.initial_lane::<LANE_SIZE_VERIFY, 0>(), found.1) {
            return Some(&found.2);
        } else {
            return None;
        }
    }


    fn fold<Acc, F: FnMut(Acc, &T) -> Acc>(&self, init: Acc, mut cb: F) -> Acc {
        self.key_vals.iter().fold(init, |acc, (_,_, v)| cb(acc, v))
    }

    fn find<F: Fn(&T) -> bool>(&self, cb: F) -> Option<&T> {
        if let Some((_, _, v)) = self.key_vals.iter().find(|(_, _, v)| cb(v)) {
            return Some(&v);
        } else {
            return None;
        }
    }
}


/// This is a much simplified verison of `micro_util::consume_string_generic` that doesn't bother dealing with escapes at all.
/// It assumes `data` starts on the byte after the opening double quote, and it simply finds the first double quote after that.
/// 
/// If you pre-check that no valid keys contain quotes (via escaping), then even if we encounter an escaped quote in the wild
/// and treat it as though it were the end of the string, we can be confident it will correctly fail to match on any of the keys.
pub fn consume_string_basic<const LANE_SIZE: usize>(data: &u8p)-> usize 
where LaneCount<LANE_SIZE> : SupportedLaneCount
{
    for (offset, lane) in data.iter_lanes::<LANE_SIZE>() {
        let quote_mask = lane.simd_eq(Simd::<u8, LANE_SIZE>::splat(b'"'));
        if let Some(at) = quote_mask.first_set() {
            return offset + at + 1;
        }
    }
    return 0; // this can only happen if the json is invalid. 
}




/// This is not massively optimised. When given a query string, it will find the terminating double quote char, dealing
/// properly with escaped quotes if requested. It then simply looks up the key in a standard hashmap with FxHash. Although
/// hash maps are fast, the other options here are even faster.
/// 
/// We could consider borrowing some of the other optimisations here too:
/// 
/// 1. The last step in a hashmap is a memcmp to validate the strings match exactly. This is pretty slow, and encurs a branch mispredict
///    if the cpu can't spot a pattern in the key lengths being queried for. In our optimised map implementations we pre-pad simd lanes
///    for fast comparisons without any branching. It might be worth doing something like that here, though that potentially means hacking
///    at the heart of the hasmap a bit, which sounds painful.
/// 2. Rather than hashing the whole query string, use the same idea as SimdPerfectScan, and work out which chars are worth considering only.
///    Then still do the full comparison afterwards. Not sure to what extent this will help, but if you start hacking the map appart, this
///    might be worth exploring.
struct HashMapFallback<T, const LANE_SIZE: usize, const SUPPORT_ESCAPED_QUOTES: bool> 
where LaneCount<LANE_SIZE>: SupportedLaneCount
{
    map: FxHashMap<Vec<u8>, T>
}

impl<T, const LANE_SIZE: usize, const SUPPORT_ESCAPED_QUOTES: bool> PrefixMap<T> for HashMapFallback<T, LANE_SIZE, SUPPORT_ESCAPED_QUOTES>
where LaneCount<LANE_SIZE>: SupportedLaneCount {
    fn try_create(key_vals: Vec<(String, T)>) -> Result<Self, Vec<(String, T)>> where Self: Sized {

        if !SUPPORT_ESCAPED_QUOTES && key_vals.iter().any(|(k, _)| k.contains('\"')) {
            return Err(key_vals);
        }
        let mut ret = HashMapFallback{
            map: FxHashMap::default()
        };
        ret.map.reserve(key_vals.len());
        for (key, val) in key_vals {
            ret.map.insert(key.as_bytes().to_vec(),  val);
        }
        return Ok(ret);
    }

    #[inline(never)]
    fn get_from_prefix(&self, data: &u8p) -> Option<&T> {
        let n_bytes = if SUPPORT_ESCAPED_QUOTES {
            micro_util::consume_string_generic::<LANE_SIZE, true>(data)
        } else {
            consume_string_basic::<LANE_SIZE>(data)
        };

        if n_bytes <= 1 {
            return None;
        }
        unsafe {
            // SAFETY: consume_string_generic would only return a value that is valid in this sense
            hint::assert_unchecked(n_bytes -1 < data.len());
        }
        let query_key = &data[..n_bytes-1];
        return self.map.get(query_key);
    }


    fn fold<Acc, F: FnMut(Acc, &T) -> Acc>(&self, init: Acc, mut cb: F) -> Acc {
        self.map.iter().fold(init, |acc, (_, v)| cb(acc, v))
    }

    fn find<F: Fn(&T) -> bool>(&self, cb: F) -> Option<&T> {
        if let Some((_, v)) = self.map.iter().find(|(_, v)| cb(v)) {
            return Some(&v);
        } else {
            return None;
        }
    }
}


const DEFAULT_LANE_SIZE_B: usize = if DEFAULT_LANE_SIZE == 16 { 32 } else {DEFAULT_LANE_SIZE};

enum AdaptivePrefixMapInner<T> {
    ExhaustiveCompare1(ExhaustiveCompare<T, 1, DEFAULT_LANE_SIZE>),
    ExhaustiveCompare2(ExhaustiveCompare<T, 2, DEFAULT_LANE_SIZE>),
    SimdPerfectScanMap(SimdPerfectScanMap<T, 16, DEFAULT_LANE_SIZE, DEFAULT_LANE_SIZE_B>),
    HashMapFallbackNoEscapes(HashMapFallback<T, DEFAULT_LANE_SIZE, false>),
    HashMapFallback(HashMapFallback<T, DEFAULT_LANE_SIZE, true>),
}


/// Designed to allow for one-time map construction (at run time), and then fast lookups in high volume.  Depending on the keys provided
/// it will pick the most optimised version it can.
/// 
/// Hopefully, the branch predictor will easily spot which version is in use when parsing a given file, including if there are nested bits
/// within the json, and thus the overhead of having different implementations will be tiny.
pub struct AdaptivePrefixMap<T: Debug>{
    inner: AdaptivePrefixMapInner<T> // private
}

impl<T: Debug> AdaptivePrefixMap<T> {
    pub fn create(key_vals: Vec<(String, T)>) -> Self {
        // the syntax here is a little clunky, but basically we allow each try_create to take ownership of key_vals, and if it's not able
        // to create an instance of the given type it spits the original key_vals back out again, so we can reuse it without any copies/clones.
        let mut key_vals = key_vals;
        match ExhaustiveCompare::<T,1, DEFAULT_LANE_SIZE>::try_create(key_vals) {
            Ok(m) => return AdaptivePrefixMap{inner: AdaptivePrefixMapInner::ExhaustiveCompare1(m)},
            Err(original) => key_vals = original
        };
        match ExhaustiveCompare::<T,2, DEFAULT_LANE_SIZE>::try_create(key_vals) {
            Ok(m) => return AdaptivePrefixMap{inner: AdaptivePrefixMapInner::ExhaustiveCompare2(m)},
            Err(original) => key_vals = original
        };
        match SimdPerfectScanMap::<T, 16, DEFAULT_LANE_SIZE, DEFAULT_LANE_SIZE_B>::try_create(key_vals)  {
            Ok(m) => return AdaptivePrefixMap{inner: AdaptivePrefixMapInner::SimdPerfectScanMap(m)},
            Err(original) => key_vals = original
        };
        match HashMapFallback::<T, DEFAULT_LANE_SIZE, false>::try_create(key_vals)  {
            Ok(m) => return AdaptivePrefixMap{inner: AdaptivePrefixMapInner::HashMapFallbackNoEscapes(m)},
            Err(original) => key_vals = original
        };
        // fallback should always return Ok
        return AdaptivePrefixMap{inner: AdaptivePrefixMapInner::HashMapFallback( HashMapFallback::<T, DEFAULT_LANE_SIZE, true>::try_create(key_vals).unwrap())};
    }

    pub fn get_from_prefix(&self, data: &u8p) -> Option<&T> {
        match &self.inner {
            AdaptivePrefixMapInner::ExhaustiveCompare1(m) => m.get_from_prefix(data),
            AdaptivePrefixMapInner::ExhaustiveCompare2(m) => m.get_from_prefix(data),
            AdaptivePrefixMapInner::SimdPerfectScanMap(m) => m.get_from_prefix(data),
            AdaptivePrefixMapInner::HashMapFallbackNoEscapes(m) => m.get_from_prefix(data),
            AdaptivePrefixMapInner::HashMapFallback(m) => m.get_from_prefix(data)
        }
    }

   pub fn fold<Acc, F: FnMut(Acc, &T) -> Acc>(&self, init: Acc, cb: F) -> Acc {
        match &self.inner {
            AdaptivePrefixMapInner::ExhaustiveCompare1(m) => m.fold(init, cb),
            AdaptivePrefixMapInner::ExhaustiveCompare2(m) =>  m.fold(init, cb),
            AdaptivePrefixMapInner::SimdPerfectScanMap(m) =>  m.fold(init, cb),
            AdaptivePrefixMapInner::HashMapFallbackNoEscapes(m) =>  m.fold(init, cb),
            AdaptivePrefixMapInner::HashMapFallback(m) =>  m.fold(init, cb),
        }
    }

    pub fn find<F: Fn(&T) -> bool>(&self, cb: F) -> Option<&T> {
        match &self.inner {
            AdaptivePrefixMapInner::ExhaustiveCompare1(m) => m.find(cb),
            AdaptivePrefixMapInner::ExhaustiveCompare2(m) =>  m.find(cb),
            AdaptivePrefixMapInner::SimdPerfectScanMap(m) =>  m.find(cb),
            AdaptivePrefixMapInner::HashMapFallbackNoEscapes(m) =>  m.find(cb),
            AdaptivePrefixMapInner::HashMapFallback(m) =>  m.find(cb),
        }
    }

    pub fn mode(&self) -> &str {
        match &self.inner {
            AdaptivePrefixMapInner::ExhaustiveCompare1(_) => "ExhaustiveCompare1",
            AdaptivePrefixMapInner::ExhaustiveCompare2(_) =>  "ExhaustiveCompare2",
            AdaptivePrefixMapInner::SimdPerfectScanMap(_) => "SimdPerfectScanMap",
            AdaptivePrefixMapInner::HashMapFallbackNoEscapes(_) => "HashMapFallbackNoEscapes",
            AdaptivePrefixMapInner::HashMapFallback(_) => "HashMapFallback",
        }
    }
}

impl<T: Debug> fmt::Debug for AdaptivePrefixMap<T>  {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let n = self.fold(0, |acc, _| acc+1);
        write!(f, "AdaptivePrefixMap[mode:{0}, size:{n}]", self.mode())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// convert a byte literal into a properly padded `u8p`
    macro_rules! u8p {
        ($x:literal) => {
            u8p::add_padding(&mut $x.into())
        }
    }
    
    #[test]
    fn test_exhaustive_compare(){
        // N: 1
        let kvs = vec![
            ("key1".to_string(), "val1".to_string()),
        ];
        let m = ExhaustiveCompare::<String, 1, DEFAULT_LANE_SIZE>::try_create(kvs);
        assert!(m.is_ok());

        let m = m.unwrap();
        assert_eq!(m.get_from_prefix(&u8p!("key1\" continued")), Some(&"val1".to_string()));
        assert_eq!(m.get_from_prefix(&u8p!("key1not\" continued")), None);
        assert_eq!(m.get_from_prefix(&u8p!("\" continued")), None);

        let kvs = vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ];
        let m = ExhaustiveCompare::<String, 1, DEFAULT_LANE_SIZE>::try_create(kvs);
        assert!(m.is_err()); // because too many keys

        let kvs = vec![
            ("123456789012345".to_string(), "val1".to_string()),
        ];
        let m = ExhaustiveCompare::<String, 1, DEFAULT_LANE_SIZE>::try_create(kvs);
        assert!(m.is_ok()); // key length can be 15 chars

        let kvs = vec![
            ("1234567890123456".to_string(), "val1".to_string()),
        ];
        let m = ExhaustiveCompare::<String, 1, DEFAULT_LANE_SIZE>::try_create(kvs);
        assert!(m.is_err()); // key length cannot be 16 chars


        // N: 2
        let kvs = vec![
            ("key_1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ];
        let m = ExhaustiveCompare::<String, 2, DEFAULT_LANE_SIZE>::try_create(kvs);
        assert!(m.is_ok());
        let m = m.unwrap();
        assert_eq!(m.get_from_prefix(&u8p!("key_1\" continued")), Some(&"val1".to_string()));
        assert_eq!(m.get_from_prefix(&u8p!("key2\" continued")), Some(&"val2".to_string()));
        assert_eq!(m.get_from_prefix(&u8p!("key2not\" continued")), None);

    }



    #[test]
    fn test_simd_perfect_scan(){
        let kvs = vec![
            ("key1".to_string(), "val1".to_string()),
            ("key1longer".to_string(), "val2".to_string()),
            ("key".to_string(), "val3".to_string()),
            ("now4".to_string(), "val4".to_string()),
        ];
        let m = SimdPerfectScanMap::<String, 16, DEFAULT_LANE_SIZE, DEFAULT_LANE_SIZE_B>::try_create(kvs);
        assert!(m.is_ok());
        let m = m.unwrap();

         
        assert_eq!(m.get_from_prefix(&u8p!("key1\" continued")), Some(&"val1".to_string()));
        assert!(m.get_from_prefix(&u8p!("key1 without terminal quote")).is_none());
        assert_eq!(m.get_from_prefix(&u8p!("key1longer\" continued")), Some(&"val2".to_string()));
        assert!(m.get_from_prefix(&u8p!("key1longer without terminal quote")).is_none());
        assert!(m.get_from_prefix(&u8p!("kon1")).is_none());
        assert_eq!(m.get_from_prefix(&u8p!("now4\"")),  Some(&"val4".to_string()));


        let kvs = vec![
            ("entity_type".to_string(), "val1".to_string()),
            ("entity_subtype".to_string(), "val2".to_string()),
            ("geometry".to_string(), "val3".to_string()),
            ("site_id".to_string(), "val4".to_string()),
            ("building_id".to_string(), "val5".to_string()),
            ("apartment_id".to_string(), "val6".to_string()),
            ("area_id".to_string(), "val7".to_string()),
        ];
        let m = SimdPerfectScanMap::<String, 16, DEFAULT_LANE_SIZE, DEFAULT_LANE_SIZE_B>::try_create(kvs);
        assert!(m.is_ok());
        let m = m.unwrap();

        assert_eq!(m.get_from_prefix(&u8p!("entity_type\" continued")), Some(&"val1".to_string()));
        assert_eq!(m.get_from_prefix(&u8p!("entity_subtype\" continued")), Some(&"val2".to_string()));
        assert_eq!(m.get_from_prefix(&u8p!("apartment_id\" continued")), Some(&"val6".to_string()));
    }

    #[test]
    fn test_consume_string_basic(){
        assert_eq!(consume_string_basic::<16>(&u8p!("12345\" continued")), 6);
        assert_eq!(consume_string_basic::<16>(&u8p!("01234 continued")), 0);
    }

    #[test]
    fn test_simd_eq_fast(){
        let a = u8p!("1234567").initial_lane::<32, 0>();
        let b = u8p!("12345678901234567890").initial_lane::<32, 0>();
        assert_eq!(simd_eq_fast(a, b, 7), true);

        let b = u8p!("123_5678901234567890").initial_lane::<32, 0>();
        assert_eq!(simd_eq_fast(a, b, 7), false);
        assert_eq!(simd_eq_fast(a, b, 4), false);
        assert_eq!(simd_eq_fast(a, b, 3), true);
    }

    #[test]
    fn test_fallback_no_escapes(){
        let kvs = vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2longer".to_string(), "val2".to_string()),
            ("kow101".to_string(), "val3".to_string()),
            ("now4".to_string(), "val4".to_string()),
        ];
        let m = HashMapFallback::<String, DEFAULT_LANE_SIZE, false>::try_create(kvs);
        assert!(m.is_ok());
        let m = m.unwrap();

        assert_eq!(m.get_from_prefix(&u8p!("key1\" continued")), Some(&"val1".to_string()));
        assert!(m.get_from_prefix(&u8p!("key1 without terminal quote")).is_none());
        assert_eq!(m.get_from_prefix(&u8p!("key2longer\" continued")), Some(&"val2".to_string()));
        assert!(m.get_from_prefix(&u8p!("key2lon__r\" continued")).is_none()); // tail doesn't match, but is terminated with " in the right place
        assert!(m.get_from_prefix(&u8p!("key2longer without terminal quote")).is_none());
        assert!(m.get_from_prefix(&u8p!("now4")).is_none());
        assert_eq!(m.get_from_prefix(&u8p!("now4\"")),  Some(&"val4".to_string()));
    }
}