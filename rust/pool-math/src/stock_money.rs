//! Exact RFQ stock valuation in USDC raw units. No floats, network or expiry policy.
use crate::{MathError, RawAmount};

#[derive(Clone, Copy)]
pub enum Side {
    Buy,
    Sell,
}

/// A positive base-ten price, parsed once at the public boundary.
pub struct Price {
    numerator: u128,
    denominator: u128,
}

impl Price {
    pub fn parse(text: &str) -> Result<Self, MathError> {
        if text.is_empty() || text.len() > 128 {
            return Err(MathError::InvalidPrice);
        }
        let text = text.strip_prefix('+').unwrap_or(text);
        let (mantissa, exponent) = match text.split_once(['e', 'E']) {
            Some((m, e)) => (m, e.parse::<i16>().map_err(|_| MathError::InvalidPrice)?),
            None => (text, 0),
        };
        let mut numerator = 0_u128;
        let mut fraction = 0_i16;
        let mut dotted = false;
        let mut digits = 0;
        for byte in mantissa.bytes() {
            if byte == b'.' && !dotted {
                dotted = true;
            } else if byte.is_ascii_digit() {
                numerator = numerator
                    .checked_mul(10)
                    .and_then(|n| n.checked_add(u128::from(byte - b'0')))
                    .ok_or(MathError::Overflow)?;
                digits += 1;
                fraction += i16::from(dotted);
            } else {
                return Err(MathError::InvalidPrice);
            }
        }
        if digits == 0 || numerator == 0 {
            return Err(MathError::InvalidPrice);
        }
        let scale = fraction.checked_sub(exponent).ok_or(MathError::Overflow)?;
        let power = 10_u128
            .checked_pow(u32::from(scale.unsigned_abs()))
            .ok_or(MathError::Overflow)?;
        let denominator = if scale < 0 {
            numerator = numerator.checked_mul(power).ok_or(MathError::Overflow)?;
            1
        } else {
            power
        };
        Ok(Self {
            numerator,
            denominator,
        })
    }
}

pub struct StockValue {
    pub quantity: RawAmount,
    pub decimals: u8,
    pub price: Price,
    pub fee_bps: u16,
    pub side: Side,
}

impl StockValue {
    pub fn calculate(self) -> Result<RawAmount, MathError> {
        if self.quantity.0 == 0 || self.fee_bps > 10000 {
            return Err(MathError::InvalidAmount);
        }
        let factor = match self.side {
            Side::Buy => 10000 + u128::from(self.fee_bps),
            Side::Sell => 10000 - u128::from(self.fee_bps),
        };
        let mut numerator = u128::from(self.quantity.0)
            .checked_mul(self.price.numerator)
            .and_then(|n| n.checked_mul(factor))
            .ok_or(MathError::Overflow)?;
        let mut denominator = self
            .price
            .denominator
            .checked_mul(10000)
            .ok_or(MathError::Overflow)?;
        // Cancel stock/USDC precision before multiplying to avoid needless overflow.
        let power = 10_u128
            .checked_pow(u32::from(self.decimals.abs_diff(6)))
            .ok_or(MathError::Overflow)?;
        if self.decimals < 6 {
            numerator = numerator.checked_mul(power).ok_or(MathError::Overflow)?;
        } else {
            denominator = denominator.checked_mul(power).ok_or(MathError::Overflow)?;
        }
        let floor = numerator / denominator;
        let value = floor
            .checked_add(u128::from(
                matches!(self.side, Side::Buy) && numerator % denominator != 0,
            ))
            .ok_or(MathError::Overflow)?;
        u64::try_from(value)
            .map(RawAmount)
            .map_err(|_| MathError::Overflow)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_price_precision_fees_and_directional_rounding() -> Result<(), MathError> {
        for (text, decimals, quantity, fees, buy, expected) in [
            ("8.903772", 6, 112_000_000, 0, true, 997_222_464),
            ("6.29", 6, 1_000_000, 0, true, 6_290_000),
            ("1e-7", 6, 1_000_000, 0, true, 1),
            ("1e-7", 6, 1_000_000, 0, false, 0),
            ("1.000001", 6, 1, 1, true, 2),
            ("1.000001", 6, 1, 1, false, 0),
            ("+1E2", 0, 1, 0, true, 100_000_000),
            ("6.1", 6, 17_910_000, 0, false, 109_251_000),
        ] {
            let job = StockValue {
                quantity: RawAmount(quantity),
                decimals,
                price: Price::parse(text)?,
                fee_bps: fees,
                side: if buy { Side::Buy } else { Side::Sell },
            };
            assert_eq!(job.calculate()?.0, expected);
        }
        Ok(())
    }

    #[test]
    fn malformed_prices_and_arithmetic_overflow_fail_closed() -> Result<(), MathError> {
        for text in [
            "", "0", "-1", "NaN", "inf", "1.2.3", "1e1e1", ".", "1_000", " 1",
        ] {
            assert!(Price::parse(text).is_err(), "accepted {text}");
        }
        for text in [
            "1e100",
            "1e-100",
            "1234567890123456789012345678901234567890",
        ] {
            assert!(Price::parse(text).is_err());
        }
        let job = StockValue {
            quantity: RawAmount(u64::MAX),
            decimals: 0,
            price: Price::parse("1e30")?,
            fee_bps: 1,
            side: Side::Buy,
        };
        assert!(matches!(job.calculate(), Err(MathError::Overflow)));
        Ok(())
    }
}
