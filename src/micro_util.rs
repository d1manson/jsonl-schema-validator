use std::simd::prelude::*;
use std::{hint, simd};
use std::simd::{u8x16, u8x32, Simd, Mask};
use std::ops::BitAnd;
use crate::u8p::{u8p, DEFAULT_LANE_SIZE};


/// Checks a specific length of bytes at the start of the json. If each byte is within the range given by the respective
/// byte in the `[lower, upper]` values (inclusive) then it returns that length of bytes.
/// 
/// For example, with `lower:"bebbo"` and `upper:"zzzzz"` 
/// - `"hello world"` => `5`
/// - `"hallo world"` => `0`
pub fn consume_within_range<const N: usize>(json: &u8p, lower: &[u8; N], upper: &[u8; N]) -> usize {

    if N < 8 {
        // i didn't actually benchmark this, but there must be some N below which SIMD isn't helpful. 
        panic!("Not implemented for small N."); // maybe could use a non-simd approach?
    } else if N <= 16 {
        let data = json.initial_lane::<16,0>();

        let mut buffer = [0; 16];
        buffer[..N].copy_from_slice(lower);
        let lower = u8x16::from(buffer);

        let mut buffer =[255; 16];
        buffer[..N].copy_from_slice(upper);
        let upper = u8x16::from(buffer);

        let check  =  data.simd_le(upper) & data.simd_ge(lower);
        return if check.all() { N } else { 0 };
    } else if N <= 32 {
        let data = json.initial_lane::<32,0>();

        let mut buffer = [0; 32];
        buffer[..N].copy_from_slice(lower);
        let lower = u8x32::from(buffer);

        let mut buffer = [255; 32];
        buffer[..N].copy_from_slice(upper);
        let upper = u8x32::from(buffer);

        let check  =  data.simd_le(upper) & data.simd_ge(lower);
        return if check.all() { N } else { 0 };
    } else {
        panic!("Not implemented"); // TODO: implement for 16 < N <= 32, and 32 < N <= 64
    }

}

/// This assumes the json is spec compliant, and the slice starts either on whitespace or on a json punctation char: `{}[],:`
/// (to be extra pedantic, the slice should not be starting within a string; it's an actual bit of punctation in the json structure).
/// 
/// Returns the number of bytes of whitespace either side of the punctation, plus the punctuation byte itself. The punctuation is actually
/// optional, it will just return the whitespace byte count if the given punctuation is not hit.
///
/// The expectation here is that (a) the number of bytes to consume is 0, 1 or 2, and (b) the whitespace format is consistent
/// within a file.  If those are both true, then there will be a tiny number of instructions that get executed, and the branch predictor
/// will do a good job of making those instructions incredibly fast. It's not that you can't use simd here, but even the most basic 4byte
/// simd would be slower on the most likely real world cases, so it doesn't seem like a good idea.
/// 
/// In some cases you may have one punctation char followed by another, potentially with whitespace between. In such cases, the second call
/// can set `CHECK_PRIOR_WHITESPACE` to `false` to avoid redundantly re-checking for whitesapce (the rest of the time you probably want it to be `true`).
/// When set to `false`, if the return value is zero that means that the given punctation was not found (since it was agreed that there was no
/// whitespace left to be consumed).
/// 
/// You can actually use this function to consume purely whitepsace if you pass `<PUNCT: 0, CHECK_PRIOR_WHITESPACE: true>`, though that's not the normal
/// way of using this.
/// 
/// //#[inline(never)]
pub fn consume_punct<const PUNCT: u8, const CHECK_PRIOR_WHITESPACE: bool>(json: &u8p) -> usize {
    let mut j = 0;

    // all whitespace in JSON is <= ' '

    if CHECK_PRIOR_WHITESPACE {
        while j<json.len() && json[j] <= b' ' {
            j += 1;
        }
    }

    if PUNCT != 0 {
        if j<json.len() && json[j] == PUNCT {
            j += 1;
        }
        while j<json.len() && json[j] <= b' ' {
            j += 1;
        }
    }
    return j;
}

/// This assumes the json slice is the start of a spec-compliant value (not neccessarily a bool, but definitely compliant value).
/// In the spec, a value that starts with 't' must be 'true', and similarlly for 'f' and false.
/// 
/// 
/// Returns 4 or 5 (the length of `b"true"`/`b"false"`) if it is a bool, otherwise 0.
pub fn consume_bool(json: &u8p) -> usize {
    let first_byte = json.raw_u8s()[0];
    return (first_byte == b't') as usize * 4 + (first_byte == b'f') as usize * 5;
}

/// This assumes the json slice is the start of a spec-compliant value (not neccessarily a null, but definitely compliant value).
/// In the spec, a value that starts with 'n' must be 'null'. 
/// 
/// Returns 4 (the length of `b"null"`) if it is a null, otherwise 0.
pub fn consume_null(json: &u8p) -> usize {
    let first_byte = json.raw_u8s()[0];
    return (first_byte == b'n') as usize * 4;
}

/// This assumes the json slice is the start of a spec-compliant value (not neccessarily a string, but definitely compliant value).
/// Returns the number of bytes in the string, or `0` if it's not a string (or the end of the string is never found, which would
/// violate our spec-compliance assumption anyway). 
/// 
/// It operates on chunks of LANE_SIZE, with no braching within each chunk, and time complexity of `log(LANE_SIZE)`.
/// For an M4 Mac, using `LANE_SIZE=16` the first chunk takes ~1.6ns, so if the string <= 16bytes it'll take 1.6ns
/// 
/// Although above we said the json slice starts at the start of a spec-compliant value, you can actually start inside the string
/// if you have already consumed the opening double quote. Use the `ALREADY_IN_STRING` flag to toggle this behaviour.
/// 
/// Note that wihtin the `consume_json_generic` function we do a lot of similar things to this, but there we want to mask out all
/// strings, anywhere in the json blob rather than finding the end of one string at a time. 
pub fn consume_string_generic<const LANE_SIZE: usize, const ALREADY_IN_STRING: bool>(json: &u8p)-> usize 
    where simd::LaneCount<LANE_SIZE> : simd::SupportedLaneCount
{
    let first_byte = json.raw_u8s()[0];
     
    if ALREADY_IN_STRING {
        if first_byte == 0 {
            return 0;
        }
    } else {
        if first_byte != b'"' {
            return 0; // not a string at all.
        }
    }

    let mut iter = json.iter_lanes::<LANE_SIZE>();
    let (mut offset, mut data) = unsafe { 
        // SAFETY: we checked that the first byte is not padding above, therefore the iterator will give us at least one iteration
        iter.next_unchecked()
    };
    let mut slash_carry  = !ALREADY_IN_STRING; // cheeky way to consider the opening quote as not being a real quote
    loop {
        let mut quote_mask = data.simd_eq(Simd::<u8, LANE_SIZE>::splat(b'"')).to_bitmask();
        let slash_mask = data.simd_eq(Simd::<u8, LANE_SIZE>::splat(b'\\')).to_bitmask();
        let slash_mask = is_odd_contiguous::<LANE_SIZE>(slash_mask, slash_carry);
        quote_mask &= !(slash_mask << 1) & !(slash_carry as u64);
        let quote_at = quote_mask.trailing_zeros();
        if quote_at != 64 {
            return quote_at as usize + 1 + offset as usize;
        }
        slash_carry = slash_mask & (1 << (LANE_SIZE - 1)) != 0; // is the leftmost bit 1?
        match iter.next() {
            Some(next) => (offset, data) = next,
            None => return 0 // this can only happen if the json is invalid. 
        }
    }
}

/// Wraps consume_string_generic with a DEFAULT_LANE_SIZE set (would be nice if rust allowed for defaults on generics!)

pub fn consume_string(json: &u8p)-> usize {
    consume_string_generic::<DEFAULT_LANE_SIZE, false>(json)
}

fn black_box_zero() -> usize {
    // read_volatile here is a trick borrowed from criterion's black_box.
    unsafe { std::ptr::read_volatile(&0) }
}


/// This assumes the json slice is the start of a spec-compliant value (not neccessarily a number value, but definitely compliant value).
/// In this case we consider any number to be a valid float64 (it might be out of range, but I'll still call that valid for a float).
/// 
/// Returns the number of bytes that the float took up in the json (i.e. number of characters), or `0` if not a numeric value.
pub fn consume_float(json: &u8p) -> usize {

    // the characters in a valid number are limited to: {+, -, ., 0-9, E, e}. (the + comes in the exponent only)
    // the characters we want to stop at are {whitespace, \{, \}, [, ], \,}   whitespces are all <= b' '
    // we also want to exclude {", n, t, f} for strings, null, true, false 
    // the valid character class is almost fully contained within the range '-':'E', which desn't include any of the other chars,
    // but '+' and 'e' can't be included without overlapping with the chars we want to exclude.

    for (offset, data) in json.iter_lanes::<16>()   {
        let good_mask = data.simd_eq(u8x16::splat(b'+'))
                                    | (data.simd_ge(u8x16::splat(b'-')) & data.simd_le(u8x16::splat(b'E'))) 
                                    | data.simd_eq(u8x16::splat(b'e'));
        let good_chars = good_mask.to_bitmask().trailing_ones();
        if good_chars < 16 {
            return offset + good_chars as usize;
        }
    }
    return 0; // shouldn't be possible in a valid json string to reach here
}

/// This assumes the json slice is the start of a spec-compliant value (not neccessarily a number value, but definitely compliant value).
/// This means we will never encounter a pathological case like a negative sign without any digits or numbers with leading zeros,
/// because these are not spec-compliant values.
///
/// It returns the number of bytes (chars) in the number if both of the following hold:
///   1. There is no decimal place or exponent.
///   2. The number is between INT64 Min and Max (inclusive).
/// 
/// It operates on a 32byte chunk, and doesn't have any branches, unless it has to deal with a number of exactly 19 digits, where it
/// then does a chunk more work, but again without braching within that.
/// 
/// TODO: if only 16 lanes are supported, it would probably be worth having a version of this that only conditionally does the
///       second lanes. I haven't done that yet as I couldn't work out how to write it conscisely to include the length 19 logic too.
pub fn consume_int64(json: &u8p) -> usize {
    const INT64_MAX: &[u8; 19] = b"9223372036854775807";
    const INT64_MIN: &[u8; 20] = b"-9223372036854775808";

    let data = json.initial_lane::<32,0>();
    let digits_mask = data.simd_ge(u8x32::splat(b'0')) & data.simd_le(u8x32::splat(b'9'));
   
    let has_sign = json.raw_u8s()[0] == b'-';
    let good_chars = ((has_sign as u64) | (digits_mask.to_bitmask() as u64)) .trailing_ones();
    let idx = good_chars as usize;
    unsafe {
        // SAFETY: we started with 32 bytes, then called .to_bitmask(), so there's a max of 32 trailing_ones
        hint::assert_unchecked(good_chars < 64);
    }
    // if 0 < good_chars <= 20, there are only three chars that can come after it - {e, E, .} - in valid json.
    let check_next_char = (json.raw_u8s()[idx] != b'e') & (json.raw_u8s()[idx] != b'E') & (json.raw_u8s()[idx] != b'.');
     
    let mut valid_mask : usize = 0xffff;

    if good_chars == 19 + (has_sign as u32) {
        let mut buffer= [0; 32];
        buffer[..19].copy_from_slice(INT64_MAX);
        let int64_max_vec = u8x32::from_slice(&buffer);

        let mut buffer = [0; 32];
        buffer[..20].copy_from_slice(INT64_MIN);
        let int64_min_vec = u8x32::from_slice(&buffer[..]);

        let bound = [int64_max_vec, int64_min_vec][has_sign as usize];
        
        let lt_bound_mask = data.simd_lt(bound);
        let gt_bound_mask = data.simd_gt(bound);

        let first_lt_bound_at = lt_bound_mask.to_bitmask().trailing_zeros() + 1;
        let first_gt_bound_at = gt_bound_mask.to_bitmask().trailing_zeros() + 1;

        //println!("json={json:?} lt_bound_mask={1:b} ({first_lt_bound_at}) gt_bound_mask={2:b} ({first_gt_bound_at})",
        //           lt_bound_mask.to_bitmask(), gt_bound_mask.to_bitmask());
    
        if (first_gt_bound_at < first_lt_bound_at) & (first_gt_bound_at <= good_chars) {
            valid_mask = black_box_zero();
        }
    
    }
    if good_chars > 19 + (has_sign as u32) {
        valid_mask = black_box_zero();
    }
    if !check_next_char {
        valid_mask = black_box_zero();
    }
    return good_chars as usize & valid_mask;

}



/// This assumes the json slice is the start of a spec-compliant value (not neccessarily a number value, but definitely a compliant value).
/// This means we will never encounter a pathological case like a negative sign without any digits or numbers with leading zeros,
/// or a backslash outside of a string because these are not spec-compliant values.
///
/// It returns the number of bytes (chars) in the number if all of the following hold:
///  1. There are at most 29 digits to the left of the decimal point (or 29 digits total if no dp).
///  2. There are at most 9 digits to the right of the decimal point. 
///  3. There is no exponent.
/// 
/// This is to match up with the BigQuery NUMERIC defaults (maybe we should carefully support exponents here?). 
/// 
/// TODO: do a version for BIGNUMERIC in BQ, the 29 & 9 become 38 & 38.
///       WARNING: this is a tiny bit too relaxed as the 77th digit is only partialy supported by BQ.
///       Maybe suoport customizing the numbers at run time - BQ does support fixing the decimal point in different places.
pub fn consume_decimal_29_9(json: &u8p) -> usize {

    let data_0_31 = json.initial_lane::<32,0>();
    let data_32_47 = json.initial_lane::<16,32>();

    let dp_mask = data_0_31.simd_eq(u8x32::splat(b'.')); // decimal place must be within first 32 chars
     // in ascii, '.' is just before '0' (actually there's '\' in between, but that wouldn't be valid json so we can ignore it)
    let digits_mask_0_31 = data_0_31.simd_ge(u8x32::splat(b'.')) & data_0_31.simd_le(u8x32::splat(b'9'));
    let digits_mask_32_47 = data_32_47.simd_ge(u8x16::splat(b'.')) & data_32_47.simd_le(u8x16::splat(b'9'));

    let has_sign = json.raw_u8s()[0] == b'-';
    let good_chars = ((has_sign as u64) | (digits_mask_0_31.to_bitmask() as u64) | ( (digits_mask_32_47.to_bitmask() as u64) << 32)) .trailing_ones();
    unsafe {
        // SAFETY: we only have data for 32+16 bytes, which we called .to_bitmask() on, so there's a max of 32+16<64 trailing_ones
        hint::assert_unchecked(good_chars < 64);
    }
    let dp_at = dp_mask.to_bitmask().trailing_zeros() as u32 + 1;

    /*
    At this point we know good_chars, and in the happy path we simply want to return that asap.
    In the unhappy path we have a JSON-compatible number that doesn't meet our criteria, and we return zero. But that can be a bit slower.

    With a naive implementation, the compiler will optimise all the following logic into a branchelss set of instructions. Normally 
    branchless is a good thing, but it means we don't benefit from the magic of the CPU's branch predictor. So instead, we force the compiler
    to use a real branch (via black_box_zero). This brings the CPU's branch predictor into the game. But the branch predictor can't do much
    if there are lots of instructions required to compute the if statement. So, instead we split up the if statement into lots of simpler
    if statements, allowing the branch predictor to do away with even more instructions.
    */

    let has_dp = dp_at < 65;
    let check_left_without_dp = good_chars <= 29 + has_sign as u32;
    let check_left_with_dp = dp_at <= 30 + has_sign as u32; 
    let check_right_with_dp = good_chars <= dp_at + 9;

    // if 0 < good_chars <= 40, there are only two chars that can come after it - {e, E} - in valid json.
    let check_next_char =  (json.raw_u8s()[good_chars as usize] != b'e') & (json.raw_u8s()[good_chars as usize] != b'E');

    let mut valid_mask : usize = 0xffff;

    if !check_next_char {
        valid_mask = black_box_zero();
    }

    if !has_dp & !check_left_without_dp {
        valid_mask = black_box_zero();
    } 
    
    if has_dp & !check_left_with_dp {
        valid_mask = black_box_zero();
    } 

    if has_dp & !check_right_with_dp {
        valid_mask = black_box_zero();
    } 

    //println!("json: {0} good_chars:{good_chars:?} check_e:{check_e:?} dp_at:{dp_at:?} check_left_without_dp:{check_left_without_dp:?}", padded_bytes_to_trimmed_string(json));

    return good_chars as usize & valid_mask;
    
    
}

/// Assumes json is valid, and is the start of a value of some kind. If it is the start of an array, it
/// finds the matching end of the array, and similarlly for start/end of objects, returning the number of
/// bytes needed to get to the end. For true/false, strings and numbers it will return the byte count (by making
/// a call to the associated `consume_*` function). For null it returns zero.
/// 
/// It properly deals with nested arrays/objects and isn't confused by strings containing brackets, even if
/// the strings have arbitrarly long escaped quote sequences. It operates on chunks of LANE_SIZE, with no
/// braching within each chunk, and time complexity of `log(LANE_SIZE)`.
/// 
/// For an M4 Mac, using `LANE_SIZE=16` the first chunk takes ~6ns, so if the whole object/array is finished
/// within 16 bytes it will take ~6ns to process.
fn consume_json_generic<const LANE_SIZE: usize>(json: &u8p) -> usize 
    where simd::LaneCount<LANE_SIZE>: simd::SupportedLaneCount 
{
    let first_byte = json.raw_u8s()[0];
    
    if first_byte | 0b0010_0000 == b'{' {
        assert_eq!(b'[' | 0b0010_0000, b'{');
        assert_eq!(b'{' | 0b0010_0000, b'{');
    } else if first_byte & 0b0110_0000 == 0b0110_0000 {
        assert_eq!(b't' & 0b0110_0000, 0b0110_0000);
        assert_eq!(b'f' & 0b0110_0000, 0b0110_0000);
        assert_eq!(b'n' & 0b0110_0000, 0b0110_0000);
        return consume_bool(json); // nulls get zero here
    } else if first_byte == b'"' {
        return consume_string_generic::<LANE_SIZE, false>(json);
    } else {
        return consume_float(json);
    }

    let mut iter = json.iter_lanes::<LANE_SIZE>();
    let (mut offset, mut data) = unsafe {
        // SAFETY: we matched the first byte on '{' or '[' above, therefore the iterator will give us at least one iteration
        iter.next_unchecked()
    };

    let open_char: u8 = first_byte;
    assert_eq!(b'{' + 2, b'}');
    assert_eq!(b'[' + 2, b']');
    let close_char: u8 = open_char + 2; // the +2 to go from open to close is (statically) asserted above

    let mut depth_carry = 0i32; // depth_carry can never go negative, but the sum on a single chunk can be negative; so easier to use sign here
    let mut quote_carry = false; 
    let mut slash_carry = false; // is the leftmost bit 1?

    loop {
        let mut quote_mask = data.simd_eq(Simd::<u8, LANE_SIZE>::splat(b'"')).to_bitmask();
        let slash_mask = data.simd_eq(Simd::<u8, LANE_SIZE>::splat(b'\\')).to_bitmask();
        let slash_mask = is_odd_contiguous::<LANE_SIZE>(slash_mask, slash_carry);
        quote_mask &= !(slash_mask << 1) & !(slash_carry as u64);
        quote_mask = is_inside::<LANE_SIZE>(quote_mask, quote_carry);

        
        let is_open = data.simd_eq(Simd::<u8, LANE_SIZE>::splat(open_char));
        let is_close = data.simd_eq(Simd::<u8, LANE_SIZE>::splat(close_char));
        let mut sum = -is_open.to_int() /* +1 for open */ + is_close.to_int() /* -1 for close */;
        sum = sum.bitand(Mask::<i8, LANE_SIZE>::from_bitmask(!quote_mask).to_int());  // ignore anything that was within a quote block
        sum = simd_cumsum(sum);   
        
        // in extreme circumstances, the depth carry could be more than 64 (or even more than an i8 can handle!), which means
        // matched_mask will be all zeros for this chunk; only a later chunk will have the possibility of an actual match.
        assert!(LANE_SIZE < 65);
        let matched_mask = sum.simd_eq(Simd::<i8, LANE_SIZE>::splat(-(depth_carry.clamp(0, 65) as i8))).to_bitmask();
        let matched_at = matched_mask.trailing_zeros();

        if matched_mask != 0 {
            return matched_at as usize + 1 + offset;
        }
        depth_carry += sum[LANE_SIZE - 1] as i32;
        quote_carry = quote_mask & (1 << (LANE_SIZE-1)) != 0; // is the leftmost bit 1?
        slash_carry = slash_mask & (1 << (LANE_SIZE-1)) != 0; // is the leftmost bit 1?
        match iter.next() {
            Some(next) => (offset, data) = next,
            None => return 0 // this can only happen if the json is invalid. 
        }
    }
}

/// Wraps `consume_json_generic` with a `DEFAULT_LANE_SIZE` set (would be nice if rust allowed for defaults on generics!)
pub fn consume_json(json: &u8p) -> usize {
    consume_json_generic::<DEFAULT_LANE_SIZE>(json)
}

/// Performs a cumulative sum within a simd lane, with time complexity `Log(LANE_SIZE)`.
pub fn simd_cumsum<const LANE_SIZE: usize>(sum: Simd<i8, LANE_SIZE>) -> Simd<i8, LANE_SIZE> 
where simd::LaneCount<LANE_SIZE>: simd::SupportedLaneCount {
    let mut sum = sum;
    sum += sum.shift_elements_right::<1>(0i8);
    sum += sum.shift_elements_right::<2>(0i8);
    sum += sum.shift_elements_right::<4>(0i8);
    sum += sum.shift_elements_right::<8>(0i8);
    if LANE_SIZE == 16 {
        return sum;
    }
    sum += sum.shift_elements_right::<16>(0i8);
    if LANE_SIZE == 32 {
        return sum;
    }
    sum += sum.shift_elements_right::<32>(0i8);
    assert_eq!(LANE_SIZE, 64);
    return sum;
}


/// Takes in a 64bit mask, and returns 1 where the given bit in the mask is at an odd position in a contiguous
/// run of 1s to the *right*, 0 otherwise.
/// 
/// Some examples:
///  - `000010` => `000010`
///  - `000110` => `000010`
///  - `011010` => `001010`
///  - `001110` => `001010`
/// 
/// if `carry` is true and the mask starts with a block of ones (on the right), that block is treated as having
///  an additional one prefix. If only the first 16bits are in use, then provide N_RIGHT_BITS=16, otherwise provide 64. 
/// 
/// Runs with time complexity `Log(N_RIGHT_BITS)`.
//#[inline(never)]
pub fn is_odd_contiguous<const N_RIGHT_BITS: usize>(mask: u64, carry: bool) -> u64 {
    let mut ret  = mask;
    let mut in_progress = mask; // 1: the corresponding bit in ret hasn't yet encountered the end of the block 

    ret &= (!0 << 1) | (!carry as u64); // if the carry flag is true, then the first bit in mask is forced to 0

    ret ^= (ret << 1) & in_progress;
    in_progress &= in_progress << 1;

    ret ^= (ret << 2) & in_progress;
    in_progress &= in_progress << 2;

    ret ^=  (ret << 4) & in_progress;
    in_progress &= in_progress << 4;

    ret ^=  (ret << 8) & in_progress;

    if N_RIGHT_BITS > 16 {
        assert_eq!(N_RIGHT_BITS, 64);
        in_progress &= in_progress << 8;

        ret ^=  (ret << 16) & in_progress;
        in_progress &= in_progress << 16;

        ret ^=  (ret << 32) & in_progress;
    }

    return ret;
}


/// Takes a 64bit mask where 1s mark the start and end of a section, starting from the right.
/// It fills the inside of each seciton with 1s, leaving the rest as 0s. The 1s block includes
/// the starting bit but not the ending bit. If you need the ending bit to be 1, you can do
/// a bitwise or of the output mask with the input mask.
/// 
/// Some examples:
///  - `000010010` => `000001110`
///  - `101001010` => `011000110`
/// 
/// if `carry` is true the first 1 is considered a section end rather than a section start.
/// You can specify N_RIGHT_BITS=16 rather than 64, to save 1 instruction, how nice ;)!
/// Runs with time complexity `~Log(N_RIGHT_BITS)`.
//#[inline(never)]
pub fn is_inside<const N_RIGHT_BITS: usize>(mask: u64, carry: bool) -> u64 {
    let mut ret  = mask;

    ret ^= ret << 1;
    ret ^= ret << 2;
    ret ^= ret << 4;
    ret ^= ret << 8;
    if N_RIGHT_BITS == 16 {
        ret ^= (carry as u64).wrapping_neg();
        ret &= 0xff_ff;
        return ret;
    } else {
        assert!(N_RIGHT_BITS <= 64);
        ret ^= ret << 16;
        ret ^= ret << 32;              
        ret ^= (carry as u64).wrapping_neg();  
        return ret;
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
    fn test_consume_json(){
        
        assert_eq!(consume_json(&u8p!(b"null")), 0);
        assert_eq!(consume_json(&u8p!(b"true")), 4);
        assert_eq!(consume_json(&u8p!(b"false")), 5);
        assert_eq!(consume_json(&u8p!(b"\"something\"")), 11);
        assert_eq!(consume_json(&u8p!(b"-0.3141e1.10")), 12);

        assert_eq!(consume_json(&u8p!(b"{  { }  { } }{ {   {")), 13);
        assert_eq!(consume_json(&u8p!(b"[  [ ]  [ ] ][ [   [")), 13);
        assert_eq!(consume_json(&u8p!(b"{\"aaaa\" { } }}{ {   {")), 13);
        assert_eq!(consume_json(&u8p!(b"{\"aa}a\" { } }}{ {   {")), 13);
        assert_eq!(consume_json(&u8p!(b"{\"\\\"}a\" { } }}{ {   {")), 13);
        assert_eq!(consume_json(&u8p!(b"{\"23456}890123456789\", {}}   ")), 26);
        assert_eq!(consume_json(&u8p!(b"{\"23456}89012345\\789\", {}}   ")), 26);
        assert_eq!(consume_json(&u8p!(b"{\"23456}89012345\\\"89\", {}}   ")), 26);
        assert_eq!(consume_json(&u8p!(b"{2{4{6{8{0{2{\"5678\"}0}2}4}6}8}0}{{{}}}")), 32);
        assert_eq!(consume_json(&u8p!(b"{\"ref\":\"wip/refactor_level_loading_code\",\"ref_type\":\"branch\",\"master_branch\":\"master\",\"description\":\"Clone is a physics based platformer. \",\"pusher_type\":\"user\"}")), 161);
    }


    #[test]
    fn test_is_odd_contiguous_64(){
        assert_eq!(is_odd_contiguous::<64>(0, false), 0);
        assert_eq!(is_odd_contiguous::<64>(0b1, false), 0b1);
        assert_eq!(is_odd_contiguous::<64>(0b1, true), 0);
        assert_eq!(is_odd_contiguous::<64>(0b1, false), 0b1);
        assert_eq!(is_odd_contiguous::<64>(0b1,false), 0b1);
        assert_eq!(is_odd_contiguous::<64>(0b011010, false), 0b1010);
        assert_eq!(is_odd_contiguous::<64>(0b001110, false), 0b1010);
        assert_eq!(is_odd_contiguous::<64>(0b11111111, false), 0b01010101);
        assert_eq!(is_odd_contiguous::<64>(0b11111111, true), 0b10101010);
        assert_eq!(is_odd_contiguous::<64>(0b11110011111111, false), 0b1010001010101);
        assert_eq!(is_odd_contiguous::<64>(!0, false), 0b0101010101010101010101010101010101010101010101010101010101010101);
    }

    
    #[test]
    fn test_is_odd_contiguous_16(){
        assert_eq!(is_odd_contiguous::<16>(0, false), 0);
        assert_eq!(is_odd_contiguous::<16>(0b1, false), 0b1);
        assert_eq!(is_odd_contiguous::<16>(0b1, true), 0);
        assert_eq!(is_odd_contiguous::<16>(0b1, false), 0b1);
        assert_eq!(is_odd_contiguous::<16>(0b1,false), 0b1);
        assert_eq!(is_odd_contiguous::<16>(0b011010, false), 0b1010);
        assert_eq!(is_odd_contiguous::<16>(0b001110, false), 0b1010);
        assert_eq!(is_odd_contiguous::<16>(0b11111111, false), 0b01010101);
        assert_eq!(is_odd_contiguous::<16>(0b11111111, true), 0b10101010);
        assert_eq!(is_odd_contiguous::<16>(0b11110011111111, false), 0b1010001010101);
    }



    #[test]
    fn test_is_inside_64() {
        assert_eq!(is_inside::<64>(0, false), 0);
        assert_eq!(is_inside::<64>(0b000010010, false), 0b0000_1110);
        assert_eq!(is_inside::<64>(0b000010010, true), (!0 << 8) | 0b1111_0001);
        assert_eq!(is_inside::<64>(0b101001010, false), 0b1100_0110);

        assert_eq!(is_inside::<64>(0b1000_0010 << 32, false), 0b0111_1110 << 32);

        assert_eq!(is_inside::<64>(1 << 63 | 1, false), !0 >> 1);
        assert_eq!(is_inside::<64>(1 << 63 | 1, true), 1 << 63);    
    }

    #[test]
    fn test_is_inside_16() {
        assert_eq!(is_inside::<16>(0, false), 0);
        assert_eq!(is_inside::<16>(0b0001_0010, false), 0b0000_1110);
        assert_eq!(is_inside::<16>(0b0001_0010, true), 0b1111_1111_1111_0001);
        assert_eq!(is_inside::<16>(0b1_0100_1010, false), 0b0_1100_0110);
        assert_eq!(is_inside::<16>(0b1000_0010_0000_0000, false), 0b0111_1110_0000_0000);
        assert_eq!(is_inside::<16>(0b1000_0000_0000_0001, false), 0b0111_1111_1111_1111);
        assert_eq!(is_inside::<16>(0b1000_0000_0000_0001, true), 0b1000_0000_0000_0000);    
    }

    #[test]
    fn test_consume_punct() {
        assert_eq!(consume_punct::<b':', true>(&u8p!(b":100")), 1);
        assert_eq!(consume_punct::<b':', true>(&u8p!(b" :null")), 2);
        assert_eq!(consume_punct::<b':', true>(&u8p!(b" \t:\"something\"")), 3);
        assert_eq!(consume_punct::<b':', true>(&u8p!(b" \t: 99")), 4);
        assert_eq!(consume_punct::<b':', true>(&u8p!(b" \t:\t\t 99")), 6);
        assert_eq!(consume_punct::<b':', true>(&u8p!(b" \t:\t\t true")), 6);
        assert_eq!(consume_punct::<b':', true>(&u8p!(b" \t:\t\t null")), 6);
        assert_eq!(consume_punct::<b':', true>(&u8p!(b" \t:\t\t false")), 6);


        assert_eq!(consume_punct::<b'}', true>(&u8p!(b"} ")), 2);
        assert_eq!(consume_punct::<b'}', true>(&u8p!(b"}} ")), 1);
        assert_eq!(consume_punct::<b'}', true>(&u8p!(b"}\t} ")), 2);
        assert_eq!(consume_punct::<b'}', true>(&u8p!(b"}\t } ")), 3);
    }

    #[test]
    fn test_consume_within_range() {
        pub const QUOTED_DATE_LOWER: &[u8; 12] = b"\"0000-00-00\"";
        pub const QUOTED_DATE_UPPER: &[u8; 12] = b"\"9999/19/39\"";


        // non-string valid values consume zero
        assert_eq!(consume_within_range(&u8p!(b"null"), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);
        assert_eq!(consume_within_range(&u8p!(b"true"), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);
        assert_eq!(consume_within_range(&u8p!(b"false"), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);
        assert_eq!(consume_within_range(&u8p!(b"1"), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);
        assert_eq!(consume_within_range(&u8p!(b"-1"), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);
        assert_eq!(consume_within_range(&u8p!(b"[false]"), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);
        assert_eq!(consume_within_range(&u8p!(b"{false}"), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);

        // within valid string values...

        // true
        assert_eq!(consume_within_range(&u8p!(b"\"2023-10-27\""), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), QUOTED_DATE_LOWER.len());
        assert_eq!(consume_within_range(&u8p!(b"\"2023/10/27\""), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), QUOTED_DATE_LOWER.len());
        assert_eq!(consume_within_range(&u8p!(b"\"2023.10.27\""), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), QUOTED_DATE_LOWER.len());  // '.' is a valid separator as it's the char between '-' and '/' in ascii
        assert_eq!(consume_within_range(&u8p!(b"\"2023-10/27\""), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), QUOTED_DATE_LOWER.len());  // WARNING: a mixed delimiter string is still valid here
        assert_eq!(consume_within_range(&u8p!(b"\"2023x10x27\""), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);  // x is not a valid separator
        
        
        assert_eq!(consume_within_range(&u8p!(b"\"2023-09-01\""), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), QUOTED_DATE_LOWER.len());
        assert_eq!(consume_within_range(&u8p!(b"\"2023-10-39\""), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), QUOTED_DATE_LOWER.len()); // WARNING: 39th of October passes our relaxed check 

        // false
        assert_eq!(consume_within_range(&u8p!(b"2023-10-27"), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0); // unqoted
        assert_eq!(consume_within_range(&u8p!(b"\"2023-10-42\""), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0); // out of range days
        assert_eq!(consume_within_range(&u8p!(b"\"2023-10-2x\""), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);
        assert_eq!(consume_within_range(&u8p!(b"\"2023-10-2x"), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);
        assert_eq!(consume_within_range(&u8p!(b"\"2023-10-20    }"), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);
        assert_eq!(consume_within_range(&u8p!(b"\"2023"), QUOTED_DATE_LOWER, QUOTED_DATE_UPPER), 0);
    }

    
    #[test]
    fn test_consume_bool(){
        // as noted in the function definition, the function assumes the input is the start of a spec-compliant JSON value

        // non-bool valid values consume zero
        assert_eq!(consume_bool(&u8p!(b"null")), 0);    
        assert_eq!(consume_bool(&u8p!(b"\"something\"")), 0);
        assert_eq!(consume_bool(&u8p!(b"\"1\"")), 0);
        assert_eq!(consume_bool(&u8p!(b"\"-1\"")), 0);
        assert_eq!(consume_bool(&u8p!(b"[false]")), 0); 
        assert_eq!(consume_bool(&u8p!(b"{\"x\": 1}")), 0); 

        // within valid bool values...
        
        assert_eq!(consume_bool(&u8p!(b"true   ")), 4);
        assert_eq!(consume_bool(&u8p!(b"false   ")), 5);
        assert_eq!(consume_bool(&u8p!(b"true, \"   ")), 4);
    }


    #[test]
    fn test_consume_decimal(){
        // as noted in the function definition, the function assumes the input is the start of a spec-compliant JSON value

        
        // non-numeric valid values consume zero
        assert_eq!(consume_decimal_29_9(&u8p!(b"null")), 0);    
        assert_eq!(consume_decimal_29_9(&u8p!(b"true")), 0);    
        assert_eq!(consume_decimal_29_9(&u8p!(b"false")), 0);   
        assert_eq!(consume_decimal_29_9(&u8p!(b"\"something\"")), 0);
        assert_eq!(consume_decimal_29_9(&u8p!(b"[false]")), 0); 
        assert_eq!(consume_decimal_29_9(&u8p!(b"{\"x\": 1}")), 0); 
        
        // within valid numeric values...
        
        assert_eq!(consume_decimal_29_9(&u8p!(b"1 ")), 1);
        assert_eq!(consume_decimal_29_9(&u8p!(b"123456789 ")), 9);
        assert_eq!(consume_decimal_29_9(&u8p!(b"12345678901234567890123456789.0 ")), 31);
        assert_eq!(consume_decimal_29_9(&u8p!(b"123, \"something\"")), 3);

        assert_eq!(consume_decimal_29_9(&u8p!(b"123.567 ")), 7);
        assert_eq!(consume_decimal_29_9(&u8p!(b"123.567, \"something\"")), 7);
        assert_eq!(consume_decimal_29_9(&u8p!(b"true ")), 0);

        
        assert_eq!(consume_decimal_29_9(&u8p!(b"-234.678 ")), 8);
        assert_eq!(consume_decimal_29_9(&u8p!(b"-234 ")), 4);

        assert_eq!(consume_decimal_29_9(&u8p!(b"-234e67 ")), 0); // numbers with e/E not considered decimals
        assert_eq!(consume_decimal_29_9(&u8p!(b"-234E67 ")), 0); // numbers with e/E not considered decimals
        assert_eq!(consume_decimal_29_9(&u8p!(b"-234E-78 ")), 0); // numbers with e/E not considered decimals

        assert_eq!(consume_decimal_29_9(&u8p!(b"12345678901234567890123456789 ")), 29); // max length
        assert_eq!(consume_decimal_29_9(&u8p!(b"-12345678901234567890123456789 ")), 30); // max length with -
        assert_eq!(consume_decimal_29_9(&u8p!(b"-12345678901234567890123456789.012345678 ")), 40); // max length with - and .
        
        assert_eq!(consume_decimal_29_9(&u8p!(b"123456789012345678901234567890 ")), 0); // too long
        assert_eq!(consume_decimal_29_9(&u8p!(b"-123456789012345678901234567890 ")), 0); // too long
        assert_eq!(consume_decimal_29_9(&u8p!(b"-123456789012345678901234567890.123456789 ")), 0); // too long on left of dp
        assert_eq!(consume_decimal_29_9(&u8p!(b"-1234567890123456789012345678901234567890.123456789 ")), 0); // too long on left of dp
        assert_eq!(consume_decimal_29_9(&u8p!(b"-12345678901234567890123456789.1234567890 ")), 0); // too long on right of dp
        
    }

    #[test]
    fn test_consume_int(){
        // as noted in the function definition, the function assumes the input is the start of a spec-compliant JSON value

        // non-numeric valid values consume zero
        assert_eq!(consume_int64(&u8p!(b"null")), 0);    
        assert_eq!(consume_int64(&u8p!(b"true")), 0);    
        assert_eq!(consume_int64(&u8p!(b"false")), 0);   
        assert_eq!(consume_int64(&u8p!(b"\"something\"")), 0);
        assert_eq!(consume_int64(&u8p!(b"[false]")), 0); 
        assert_eq!(consume_int64(&u8p!(b"{\"x\": 1}")), 0); 

        // within valid numeric values...

        assert_eq!(consume_int64(&u8p!(b"1 ")), 1);
        assert_eq!(consume_int64(&u8p!(b"0 ")), 1);
        assert_eq!(consume_int64(&u8p!(b"123456789012345678 ")), 18); // 18 digits
        assert_eq!(consume_int64(&u8p!(b"-123456789012345678 ")), 19); // negativ with 18 digits 

        assert_eq!(consume_int64(&u8p!(b"9223372036854775807,")), 19); // 19 digits, exactly int max
        assert_eq!(consume_int64(&u8p!(b"-9223372036854775808}")), 20); // 19 digits, exactly int min
        

        assert_eq!(consume_int64(&u8p!(b"9223372036854775808,")), 0); // 19 digits, exactly int max + 1
        assert_eq!(consume_int64(&u8p!(b"-9223372036854775809}")), 0); // 19 digits, exactly int min - 1

        assert_eq!(consume_int64(&u8p!(b"9223372036854775817,")), 0); // 19 digits, exactly int max + 10
        assert_eq!(consume_int64(&u8p!(b"-9223372036854775818}")), 0); // 19 digits, exactly int min - 10


        assert_eq!(consume_int64(&u8p!(b"9993372036854775817,")), 0); // 19 digits, quite a bit above int max
        assert_eq!(consume_int64(&u8p!(b"-9993372036854775818}")), 0); // 19 digits, quite a bit below int min
        // todo: test the 19 digit number
        assert_eq!(consume_int64(&u8p!(b"12345678901234567890 ")), 0);
        assert_eq!(consume_int64(&u8p!(b"-12345678901234567890 ")), 0);


        assert_eq!(consume_int64(&u8p!(b"12345.678901 ")), 0); // decimal is not int
        assert_eq!(consume_int64(&u8p!(b"-12345.678901 ")), 0);
        assert_eq!(consume_int64(&u8p!(b"123e+6 ")), 0); // number with exponent is not int
        assert_eq!(consume_int64(&u8p!(b"-123E-6 ")), 0);        
    }

    #[test]
    fn test_consume_float(){   
        // non-numeric valid values consume zero
        assert_eq!(consume_float(&u8p!(b"null")), 0);    
        assert_eq!(consume_float(&u8p!(b"true")), 0);    
        assert_eq!(consume_float(&u8p!(b"false")), 0);   
        assert_eq!(consume_float(&u8p!(b"\"something\"")), 0);
        assert_eq!(consume_float(&u8p!(b"[false]")), 0); 
        assert_eq!(consume_float(&u8p!(b"{\"x\": 1}")), 0); 

        // within valid numeric values...

        assert_eq!(consume_float(&u8p!(b"1")), 1);  
        assert_eq!(consume_float(&u8p!(b"12")), 2);
        assert_eq!(consume_float(&u8p!(b"12}")), 2);
        assert_eq!(consume_float(&u8p!(b"12]")), 2);
        assert_eq!(consume_float(&u8p!(b"12,")), 2);
        assert_eq!(consume_float(&u8p!(b"12 ")), 2);

        assert_eq!(consume_float(&u8p!(b"123456789012345, ")), 15);       
        assert_eq!(consume_float(&u8p!(b"123456789012345} ")), 15);     
        assert_eq!(consume_float(&u8p!(b"123456789012345 ")), 15);   
        assert_eq!(consume_float(&u8p!(b"1234567890123456, ")), 16);       
        assert_eq!(consume_float(&u8p!(b"1234567890123456 ")), 16);       
        assert_eq!(consume_float(&u8p!(b"1234567890123456} ")), 16);     
        assert_eq!(consume_float(&u8p!(b"1234567890123456] ")), 16);     
        assert_eq!(consume_float(&u8p!(b"12345678901234567 ")), 17);     
        assert_eq!(consume_float(&u8p!(b"12345678901234567, ")), 17);       
        assert_eq!(consume_float(&u8p!(b"12345678901234567} ")), 17);
        assert_eq!(consume_float(&u8p!(b"12345678901234567] ")), 17);       
        assert_eq!(consume_float(&u8p!(b"12345678901.345678 ")), 18);
        assert_eq!(consume_float(&u8p!(b"-2345678901.3456789 ")), 19);
        assert_eq!(consume_float(&u8p!(b"-2345678901.34567e+012 ")), 22);
        assert_eq!(consume_float(&u8p!(b"-2345678901.34567e-012 ")), 22);
        assert_eq!(consume_float(&u8p!(b"-2345678901.34567E+012 ")), 22);
        assert_eq!(consume_float(&u8p!(b"-2345678901.34567E-012 ")), 22);

    }

    #[test]
    fn test_consume_string(){
        assert_eq!(consume_string(&u8p!(b"null")), 0);    
        assert_eq!(consume_string(&u8p!(b"true")), 0);
        assert_eq!(consume_string(&u8p!(b"false")), 0);   
        assert_eq!(consume_string(&u8p!(b"1")), 0);
        assert_eq!(consume_string(&u8p!(b"-1")), 0);
        assert_eq!(consume_string(&u8p!(b"[false]")), 0); 
        assert_eq!(consume_string(&u8p!(b"{\"x\": 1}")), 0); 

        assert_eq!(consume_string(&u8p!(b"\"so\\\"methi\\ng\"   ")), 14);
        assert_eq!(consume_string(&u8p!(b"\"so\\\\\"   ")), 6);
        assert_eq!(consume_string(&u8p!(b"\"something\", ")), 11); 
        assert_eq!(consume_string(&u8p!(b"\"something\"]")), 11);
        assert_eq!(consume_string(&u8p!(b"\"something\"}")), 11);
        assert_eq!(consume_string(&u8p!(b"\"something\" ")), 11);
        assert_eq!(consume_string(&u8p!(b"\"something\"\t")), 11);
        assert_eq!(consume_string(&u8p!(b"\"12345678901234567890\"    ")), 22);
        assert_eq!(consume_string(&u8p!(b"\"123456789012345\"    ")), 17);
        assert_eq!(consume_string(&u8p!(b"\"12345678901234\\\"7890\"    ")), 22);
        assert_eq!(consume_string(&u8p!(b"\"1234567890123\\\\\"    ")), 17);
        assert_eq!(consume_string(&u8p!(b"\"12345678901234\\6789012\\\"5678\"    ")), 30);
    }

 
}