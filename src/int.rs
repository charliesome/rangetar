use std::cmp;
use std::convert::TryFrom;

pub fn usize_to_u64(int: usize) -> u64 {
    u64::try_from(int).expect("usize -> u64")
}

pub fn converting_min(a: u64, b: usize) -> usize {
    match usize::try_from(a) {
        Ok(a) => cmp::min(a, b),
        Err(_) => {
            // the u64 was too large for usize, so b must be smaller
            b
        }
    }
}
