use core::fmt;
use std::ops::Deref;
use std::simd::{LaneCount, SupportedLaneCount, Simd};
use std::hint;


const fn get_arch_lane_size() -> usize {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
    { return 64; }
    
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512bw")))]
    { return 32; }
    
    16
}


/// The DEFAULT_LANE_SIZE here is the largest that's likely available on the target arch. Although std::simd can
///  emulate any lane size up to 64, the performance doesn't seem to be better than simply doing multiple iterations
/// of a loop with a smaller lane size (at least for an M4 Mac). And indeed, most of the use cases here aren't 
/// expected to be very wide in real world use cases, so it only makes sense to go wide when it's just as cheap
/// as going narrow.
pub const DEFAULT_LANE_SIZE: usize = get_arch_lane_size();



/// The `u8p` struct wraps a `&[u8]`, where the last 64 bytes are zero **P**adding. It can be constructed as:
/// 
///     let x = u8p::add_padding(&mut some_u8vec);
/// 
///  Note the `some_u8vec` will be extended with padding, which may or may not actually require 
///  reallocating memory (depending on the existing capacity in the vector).
/// 
/// If you treat it as a `&[u8]` directly you only access the unpadded section of bytes.
/// 
///     let y = x[..]; // y does not include the zero padding 
/// 
/// You can create a new `u8p` starting part-way through the original buffer:
/// 
///     let y = x.offset(42); // will panic if 42 is beyond the proper part of the data
///  
/// The actual SIMD logic is exposed via the `.initial_lane`, and `.iter_lanes` methods. You can also access the first bytes
/// as a `[u8; PADDING_SIZE]` without any checks via `.inital_u8s`.
/// 
#[allow(non_camel_case_types)]
pub struct u8p<'a> {
    buffer: &'a [u8]
}



impl<'a> u8p<'a> {
    pub const PADDING_SIZE: usize = 64; 


    /// Extends the vector with 64 zero bytes (which may or may not require reallocating, depending on the vector's capacity),
    /// and then wraps a `u8p` around the bytes.
    pub fn add_padding(v : &mut  Vec<u8>) -> u8p {
        v.extend([0; Self::PADDING_SIZE]);
        // assert!(buffer.len() >= PADDING_SIZE) is redundant given extend above.
        u8p{buffer: v}
    }

    /// Creates a new `u8p`, starting at offset byte within the current `u8p`
    pub fn offset(&self, offset: usize) -> u8p<'a>{
        assert!(offset + Self::PADDING_SIZE <= self.buffer.len(), "u8p.offset called with remaining length less than the required padding.");
        unsafe {
            // SAFETY: the compiler should be able to see this based on the above, but it doesn't
            hint::assert_unchecked(offset < self.buffer.len());
        }
        let buffer =  &self.buffer[offset..];
        u8p{buffer}
    }

    /// Iterator returning `simd::Simd<u8, LANE_SIZE>`s, the last chunk will include zero padding (unless the buffer
    /// length is a multiple of LANE_SIZE)
    pub fn iter_lanes<const LANE_SIZE: usize>(&self) ->  u8pIter<'a, LANE_SIZE> 
    where LaneCount<LANE_SIZE>: SupportedLaneCount 
    {
        u8pIter::<LANE_SIZE>{buffer: self.buffer, offset: 0}
    }

    /// This reminds the compiler that the return value is at least PADDING_SIZE, which might help optimise things a bit
    /// but it will include all the zero padding.
    pub fn raw_u8s(&self) -> &[u8] {
        unsafe { 
            // SAFETY: condition is always enforced at construction of u8p
            hint::assert_unchecked(self.buffer.len() >= Self::PADDING_SIZE)
        };
        self.buffer
    } 
    

    /// This is guaranteed to not panic, but it could be all or partly zero padding
    pub fn initial_lane<const LANE_SIZE: usize, const OFFSET: usize>(&self) -> Simd<u8, LANE_SIZE>
    where LaneCount<LANE_SIZE>: SupportedLaneCount
    {
        assert!(OFFSET + LANE_SIZE <= Self::PADDING_SIZE);
        unsafe {
            // SAFETY: OFFSET + LANE_SIZE <= PADDING_SIZE <= self.buffer.len()
            //         the first inequality is enforced by the assertion above, the second inequality is eforced when constructing u8p's.                
            hint::assert_unchecked(OFFSET + LANE_SIZE <= self.buffer.len());
        }
        Simd::<u8, LANE_SIZE>::from_slice(&self.buffer[OFFSET..OFFSET + LANE_SIZE])
    }
}

impl<'a> Deref for u8p<'a> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe {
            // SAFETY: condition is always enforced at construction of u8p
            hint::assert_unchecked(self.buffer.len() >= Self::PADDING_SIZE) 
        };
        &self.buffer[..self.buffer.len() - Self::PADDING_SIZE]
    }    
}

impl<'a> fmt::Debug for u8p<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{0:?}+[0; {1}]", String::from_utf8(self.buffer[0..self.buffer.len() - Self::PADDING_SIZE].to_vec()).unwrap(), Self::PADDING_SIZE)
    }
}

#[allow(non_camel_case_types)]
pub struct u8pIter<'a, const LANE_SIZE: usize>
where LaneCount<LANE_SIZE>: SupportedLaneCount 
{
    buffer: &'a [u8],
    offset: usize
}

impl<'a, const LANE_SIZE: usize> Iterator for u8pIter<'a, LANE_SIZE> 
where LaneCount<LANE_SIZE>: SupportedLaneCount 
{
    type Item = (usize, Simd<u8, LANE_SIZE>);
    fn next(&mut self) -> Option<Self::Item>{
        assert!(LANE_SIZE <= u8p::PADDING_SIZE);
        if self.offset + u8p::PADDING_SIZE < self.buffer.len() {
            let slc_start = self.offset;
            let slc_end = slc_start + LANE_SIZE;    
            unsafe {
                // SAFETY: self.offset + LANE_SIZE <= self.offset + PADDING_SIZE < self.buffer.len()
                //         can be inferred from the assert and if statement above, but the compiler doesn't spot this.
                hint::assert_unchecked(slc_end < self.buffer.len());
                // SAFETY: LANE_SIZE is a usize, so not sure why the compiler struggles with this one
                hint::assert_unchecked(slc_start <= slc_end);
            }
            let ret =(self.offset, Simd::<u8, LANE_SIZE>::from_slice(&self.buffer[slc_start..slc_end]));
            self.offset += LANE_SIZE;
            return Some(ret);
        } else {
            None
        }
    }
}

impl<'a, const LANE_SIZE: usize> u8pIter<'a, LANE_SIZE> 
where LaneCount<LANE_SIZE>: SupportedLaneCount 
{ 
    /// If you somehow know that the first byte at the iterator's current offset is definitely not a padding byte
    /// then you can call this to avoid a bit of checking.
    pub unsafe fn next_unchecked(&mut self) -> (usize, Simd<u8, LANE_SIZE>) {
        unsafe { hint::assert_unchecked(self.offset + u8p::PADDING_SIZE < self.buffer.len());}
        self.next().unwrap()
    }
}


/// For use primarily in benchmarks where you want to construct the padded buffer in one place
/// and then access it as a u8p later, with zero cost when accessing it. 
#[allow(non_camel_case_types)]
pub struct u8pOwned {
    buffer: Vec<u8>
}

impl u8pOwned {
    pub fn from<T: Into<Vec<u8>>>(t: T) -> u8pOwned {
        let mut buffer : Vec<u8> = t.into();
        u8p::add_padding(&mut buffer);
        u8pOwned { buffer }
    }
    pub fn as_borrowed(&self) -> u8p {
        // we already know the buffer is padded, so no need to do any checks here
        u8p { buffer: &self.buffer }
    }
}



