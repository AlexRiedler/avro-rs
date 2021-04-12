use crate::{AvroResult, Error};
use num_bigint::{BigInt, Sign};

#[derive(Debug, Clone)]
pub struct Decimal {
    pub value: BigInt,
    pub precision: usize,
    pub scale: usize,
}

// precision does not matter, only need to check if the value scaled by scale, makes the the values equal
impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        if self.scale == other.scale {
            self.value == other.value
        } else if self.scale > rhs.scale {
            let scaled_value = &rhs.value * BigInt::from(10u64.pow(self.scale - rhs.scale)); // TODO: can this overflow
            self.value == scaled_value
        } else { // self.scale < rhs.scale
            let scaled_value = &self.value * BigInt::from(10u64.pow(rhs.scale - self.scale)); // TODO: can this overflow
            scaled_value == other.value
        }
    }
}

impl Decimal {
    pub(crate) fn to_sign_extended_bytes_with_len(&self, len: usize) -> AvroResult<Vec<u8>> {
        let sign_byte = 0xFF * u8::from(self.value.sign() == Sign::Minus);
        let mut decimal_bytes = vec![sign_byte; len];
        let raw_bytes = self.value.to_signed_bytes_be();
        let num_raw_bytes = raw_bytes.len();
        let start_byte_index = len.checked_sub(num_raw_bytes).ok_or(Error::SignExtend {
            requested: len,
            needed: num_raw_bytes,
        })?;
        decimal_bytes[start_byte_index..].copy_from_slice(&raw_bytes);
        Ok(decimal_bytes)
    }
}
