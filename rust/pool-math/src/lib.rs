//! Bounded, deterministic integer kernels. No RPC, SQL, wallet or environment access.
#![forbid(unsafe_code)]

use num_bigint::BigUint;
use num_traits::ToPrimitive;
use rayon::prelude::*;
use thiserror::Error;

pub mod stock_money;

const FEE_DENOMINATOR: u128 = 1_000_000;
pub const MAX_BATCH: usize = 100_000;
pub type IntervalResult = Result<(u128, u128), MathError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MathError {
    #[error("invalid_amount")]
    InvalidAmount,
    #[error("invalid_fee")]
    InvalidFee,
    #[error("fees_consume_input")]
    FeesConsumeInput,
    #[error("zero_output")]
    ZeroOutput,
    #[error("overflow")]
    Overflow,
    #[error("invalid_interval")]
    InvalidInterval,
    #[error("batch_too_large")]
    BatchTooLarge,
    #[error("invalid_price")]
    InvalidPrice,
}

#[derive(Clone, Copy)]
pub struct RawAmount(pub u64);

#[derive(Clone, Copy)]
pub struct CpmmInput {
    pub amount: RawAmount,
    pub reserve_in: RawAmount,
    pub reserve_out: RawAmount,
    pub trade_fee_rate: u32,
    pub creator_fee_rate: u32,
    pub creator_on_input: bool,
    pub input_transfer_fee: RawAmount,
    pub output_transfer_bps: u16,
    pub output_transfer_maximum: RawAmount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpmmOutput {
    pub input: u64,
    pub received: u64,
    pub trade_fee: u64,
    pub creator_fee: u64,
    pub input_transfer_fee: u64,
    pub output_transfer_fee: u64,
    pub pool_output: u64,
    pub output: u64,
}

/// Immutable validated pool frame, shared by all amounts quoted at this snapshot.
#[derive(Clone, Copy)]
pub struct CpmmPool {
    pub reserve_a: RawAmount,
    pub reserve_b: RawAmount,
    pub trade_fee_rate: u32,
    pub creator_fee_rate: u32,
    pub fee_on: u8,
    pub transfer_a_bps: u16,
    pub transfer_a_max: RawAmount,
    pub transfer_b_bps: u16,
    pub transfer_b_max: RawAmount,
}

impl CpmmPool {
    pub fn validate(self) -> Result<Self, MathError> {
        if self.reserve_a.0 == 0 || self.reserve_b.0 == 0 {
            return Err(MathError::InvalidAmount);
        }
        if u128::from(self.trade_fee_rate) + u128::from(self.creator_fee_rate) >= FEE_DENOMINATOR
            || self.fee_on > 2
            || self.transfer_a_bps > 10000
            || self.transfer_b_bps > 10000
        {
            return Err(MathError::InvalidFee);
        }
        Ok(self)
    }

    pub fn quote(self, a_to_b: bool, amount: u64, slippage: u16) -> Result<(u64, u64), MathError> {
        if slippage > 500 {
            return Err(MathError::InvalidFee);
        }
        let (reserve_in, reserve_out, in_bps, in_max, out_bps, out_max) = if a_to_b {
            (
                self.reserve_a,
                self.reserve_b,
                self.transfer_a_bps,
                self.transfer_a_max,
                self.transfer_b_bps,
                self.transfer_b_max,
            )
        } else {
            (
                self.reserve_b,
                self.reserve_a,
                self.transfer_b_bps,
                self.transfer_b_max,
                self.transfer_a_bps,
                self.transfer_a_max,
            )
        };
        let transfer_in = narrow(
            ceil_div(u128::from(amount) * u128::from(in_bps), 10000)?.min(u128::from(in_max.0)),
        )?;
        let q = cpmm_exact_in(CpmmInput {
            amount: RawAmount(amount),
            reserve_in,
            reserve_out,
            trade_fee_rate: self.trade_fee_rate,
            creator_fee_rate: self.creator_fee_rate,
            creator_on_input: self.fee_on == 0 || self.fee_on == if a_to_b { 1 } else { 2 },
            input_transfer_fee: RawAmount(transfer_in),
            output_transfer_bps: out_bps,
            output_transfer_maximum: out_max,
        })?;
        Ok((
            q.output,
            narrow(u128::from(q.output) * (10000 - u128::from(slippage)) / 10000)?,
        ))
    }
}

#[derive(Clone, Copy)]
pub struct PoolJob {
    pub index: usize,
    pub a_to_b: bool,
    pub amount: u64,
    pub slippage: u16,
}

pub fn prepared_quotes(
    calculator: &Calculator,
    pools: &[CpmmPool],
    jobs: Vec<PoolJob>,
    parallel: bool,
) -> Result<Vec<IntervalResult64>, MathError> {
    if jobs.len() > MAX_BATCH {
        return Err(MathError::BatchTooLarge);
    }
    let quote = |job: PoolJob| {
        pools.get(job.index).ok_or(MathError::InvalidAmount)?.quote(
            job.a_to_b,
            job.amount,
            job.slippage,
        )
    };
    Ok(if parallel {
        calculator
            .pool
            .install(|| jobs.into_par_iter().with_min_len(256).map(quote).collect())
    } else {
        jobs.into_iter().map(quote).collect()
    })
}

pub type IntervalResult64 = Result<(u64, u64), MathError>;
pub type GridResult = Result<(u64, u64, u64), MathError>;

/// Generate the sizing grid in Rust, avoiding O(pools * sizes) Python job objects.
pub fn prepared_grid(
    calculator: &Calculator,
    pools: &[CpmmPool],
    seeds: Vec<(usize, bool, u64)>,
    multipliers: &[u32],
    slippage: u16,
    parallel: bool,
) -> Result<Vec<GridResult>, MathError> {
    let count = seeds
        .len()
        .checked_mul(multipliers.len())
        .ok_or(MathError::BatchTooLarge)?;
    if count > MAX_BATCH {
        return Err(MathError::BatchTooLarge);
    }
    if slippage > 500 || multipliers.contains(&0) {
        return Err(MathError::InvalidFee);
    }
    let quote_seed = |(index, direction, base): (usize, bool, u64)| -> Vec<GridResult> {
        multipliers
            .iter()
            .map(|m| {
                let amount = base.checked_mul(u64::from(*m)).ok_or(MathError::Overflow)?;
                let (out, minimum) = pools
                    .get(index)
                    .ok_or(MathError::InvalidAmount)?
                    .quote(direction, amount, slippage)?;
                Ok((amount, out, minimum))
            })
            .collect()
    };
    let rows: Vec<Vec<GridResult>> = if parallel {
        calculator.pool.install(|| {
            seeds
                .into_par_iter()
                .with_min_len(8)
                .map(quote_seed)
                .collect()
        })
    } else {
        seeds.into_iter().map(quote_seed).collect()
    };
    Ok(rows.into_iter().flatten().collect())
}

impl MathError {
    pub fn code(self) -> u8 {
        match self {
            Self::InvalidAmount => 1,
            Self::InvalidFee => 2,
            Self::FeesConsumeInput => 3,
            Self::ZeroOutput => 4,
            Self::Overflow => 5,
            Self::InvalidInterval => 6,
            Self::BatchTooLarge => 7,
            Self::InvalidPrice => 8,
        }
    }
}

fn ceil_div(value: u128, divisor: u128) -> Result<u128, MathError> {
    if divisor == 0 {
        return Err(MathError::InvalidFee);
    }
    let quotient = value / divisor;
    quotient
        .checked_add(u128::from(value % divisor != 0))
        .ok_or(MathError::Overflow)
}

fn narrow(value: u128) -> Result<u64, MathError> {
    u64::try_from(value).map_err(|_| MathError::Overflow)
}

pub fn cpmm_exact_in(input: CpmmInput) -> Result<CpmmOutput, MathError> {
    let amount = u128::from(input.amount.0);
    let reserve_in = u128::from(input.reserve_in.0);
    let reserve_out = u128::from(input.reserve_out.0);
    let transfer_in = u128::from(input.input_transfer_fee.0);
    if amount == 0 || reserve_in == 0 || reserve_out == 0 || transfer_in >= amount {
        return Err(MathError::InvalidAmount);
    }
    let trade_rate = u128::from(input.trade_fee_rate);
    let creator_rate = u128::from(input.creator_fee_rate);
    if trade_rate >= FEE_DENOMINATOR
        || creator_rate >= FEE_DENOMINATOR
        || (input.creator_on_input && trade_rate + creator_rate >= FEE_DENOMINATOR)
        || input.output_transfer_bps > 10_000
    {
        return Err(MathError::InvalidFee);
    }
    let received = amount - transfer_in;
    let (trade_fee, mut creator_fee) = if input.creator_on_input {
        let rate = trade_rate + creator_rate;
        let total = ceil_div(received * rate, FEE_DENOMINATOR)?;
        let creator = if rate == 0 {
            0
        } else {
            total * creator_rate / rate
        };
        (total - creator, creator)
    } else {
        (ceil_div(received * trade_rate, FEE_DENOMINATOR)?, 0)
    };
    let net = received
        .checked_sub(trade_fee + creator_fee)
        .filter(|n| *n > 0)
        .ok_or(MathError::FeesConsumeInput)?;
    // u64 * u64 fits u128; the denominator is computed in u128 as well.
    let pool_output = net.checked_mul(reserve_out).ok_or(MathError::Overflow)? / (reserve_in + net);
    if !input.creator_on_input {
        creator_fee = ceil_div(pool_output * creator_rate, FEE_DENOMINATOR)?;
    }
    let before_transfer = pool_output
        - if input.creator_on_input {
            0
        } else {
            creator_fee
        };
    let transfer_out = ceil_div(
        before_transfer * u128::from(input.output_transfer_bps),
        10_000,
    )?
    .min(u128::from(input.output_transfer_maximum.0));
    let output = before_transfer
        .checked_sub(transfer_out)
        .filter(|n| *n > 0)
        .ok_or(MathError::ZeroOutput)?;
    Ok(CpmmOutput {
        input: input.amount.0,
        received: narrow(received)?,
        trade_fee: narrow(trade_fee)?,
        creator_fee: narrow(creator_fee)?,
        input_transfer_fee: input.input_transfer_fee.0,
        output_transfer_fee: narrow(transfer_out)?,
        pool_output: narrow(pool_output)?,
        output: narrow(output)?,
    })
}

/// ONE fee-free constant-liquidity interval; never a complete CLMM swap quote.
pub fn clmm_interval(liquidity: u128, start: u128, end: u128) -> Result<(u128, u128), MathError> {
    if liquidity == 0 || start == 0 || end == 0 || start == end {
        return Err(MathError::InvalidInterval);
    }
    let low = BigUint::from(start.min(end));
    let high = BigUint::from(start.max(end));
    let delta = &high - &low;
    let q64 = BigUint::from(1_u128 << 64);
    let numerator_a = BigUint::from(liquidity) * &q64 * &delta;
    let numerator_b = BigUint::from(liquidity) * delta;
    let denominator_a = high * low;
    let (numerator_in, denominator_in, numerator_out, denominator_out) = if end < start {
        (numerator_a, denominator_a, numerator_b, q64)
    } else {
        (numerator_b, q64, numerator_a, denominator_a)
    };
    // BigUint is bounded here by fixed u128 inputs (at most 384-bit intermediates).
    let input = (numerator_in + &denominator_in - BigUint::from(1_u8)) / denominator_in;
    let output = numerator_out / denominator_out;
    Ok((
        input.to_u128().ok_or(MathError::Overflow)?,
        output.to_u128().ok_or(MathError::Overflow)?,
    ))
}

pub struct Calculator {
    pool: rayon::ThreadPool,
}

impl Calculator {
    pub fn new(threads: usize) -> Result<Self, String> {
        if !(1..=16).contains(&threads) {
            return Err("threads must be 1..16".to_owned());
        }
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("pool-math-{i}"))
            .build()
            .map(|pool| Self { pool })
            .map_err(|e| format!("worker initialization: {e}"))
    }

    pub fn cpmm_batch(
        &self,
        inputs: Vec<CpmmInput>,
        parallel: bool,
    ) -> Result<Vec<Result<CpmmOutput, MathError>>, MathError> {
        if inputs.len() > MAX_BATCH {
            return Err(MathError::BatchTooLarge);
        }
        // Indexed par_iter preserves input order; one bad row doesn't cancel other quotes.
        Ok(if parallel {
            self.pool
                .install(|| inputs.into_par_iter().map(cpmm_exact_in).collect())
        } else {
            inputs.into_iter().map(cpmm_exact_in).collect()
        })
    }

    pub fn interval_batch(
        &self,
        inputs: Vec<(u128, u128, u128)>,
        parallel: bool,
    ) -> Result<Vec<IntervalResult>, MathError> {
        if inputs.len() > MAX_BATCH {
            return Err(MathError::BatchTooLarge);
        }
        Ok(if parallel {
            self.pool.install(|| {
                inputs
                    .into_par_iter()
                    .map(|(l, s, e)| clmm_interval(l, s, e))
                    .collect()
            })
        } else {
            inputs
                .into_iter()
                .map(|(l, s, e)| clmm_interval(l, s, e))
                .collect()
        })
    }
}

#[cfg(feature = "python")]
mod binding {
    use super::*;
    use pyo3::exceptions::{PyRuntimeError, PyValueError};
    use pyo3::prelude::*;
    use std::sync::Arc;

    type MoneyRow = (u64, u8, String, u16, bool);

    #[pyfunction]
    fn stock_values(py: Python<'_>, rows: Vec<MoneyRow>) -> PyResult<Vec<(u64, u8)>> {
        if rows.len() > MAX_BATCH {
            return Err(PyValueError::new_err("batch_too_large"));
        }
        Ok(py.detach(|| {
            rows.into_iter()
                .map(|(quantity, decimals, text, fee_bps, buy)| {
                    let result = stock_money::Price::parse(&text).and_then(|price| {
                        stock_money::StockValue {
                            quantity: RawAmount(quantity),
                            decimals,
                            price,
                            fee_bps,
                            side: if buy {
                                stock_money::Side::Buy
                            } else {
                                stock_money::Side::Sell
                            },
                        }
                        .calculate()
                    });
                    match result {
                        Ok(value) => (value.0, 0),
                        Err(error) => (0, error.code()),
                    }
                })
                .collect()
        }))
    }

    type InputRow = (u64, u64, u64, u32, u32, bool, u64, u16, u64);
    type OutputRow = (u64, u64, u64, u64, u64, u64, u64, u64);
    type QuoteRows = Vec<(Option<OutputRow>, Option<String>)>;
    type IntervalRows = Vec<(Option<(u128, u128)>, Option<String>)>;

    #[pyclass(frozen)]
    struct PoolMath {
        // PoolBooks share only this immutable worker pool, never trading balances.
        inner: Arc<Calculator>,
    }

    #[pymethods]
    impl PoolMath {
        #[new]
        #[pyo3(signature = (threads=4))]
        fn new(threads: usize) -> PyResult<Self> {
            Calculator::new(threads)
                .map(|inner| Self {
                    inner: Arc::new(inner),
                })
                .map_err(PyRuntimeError::new_err)
        }

        #[pyo3(signature = (rows, parallel=false))]
        fn cpmm_batch(
            &self,
            py: Python<'_>,
            rows: Vec<InputRow>,
            parallel: bool,
        ) -> PyResult<QuoteRows> {
            if rows.len() > MAX_BATCH {
                return Err(PyValueError::new_err("batch_too_large"));
            }
            let inputs = rows
                .into_iter()
                .map(
                    |(
                        amount,
                        reserve_in,
                        reserve_out,
                        trade_fee_rate,
                        creator_fee_rate,
                        creator_on_input,
                        input_transfer_fee,
                        output_transfer_bps,
                        output_transfer_maximum,
                    )| CpmmInput {
                        amount: RawAmount(amount),
                        reserve_in: RawAmount(reserve_in),
                        reserve_out: RawAmount(reserve_out),
                        trade_fee_rate,
                        creator_fee_rate,
                        creator_on_input,
                        input_transfer_fee: RawAmount(input_transfer_fee),
                        output_transfer_bps,
                        output_transfer_maximum: RawAmount(output_transfer_maximum),
                    },
                )
                .collect();
            let results = py
                .detach(|| self.inner.cpmm_batch(inputs, parallel))
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            Ok(results
                .into_iter()
                .map(|row| match row {
                    Ok(q) => (
                        Some((
                            q.input,
                            q.received,
                            q.trade_fee,
                            q.creator_fee,
                            q.input_transfer_fee,
                            q.output_transfer_fee,
                            q.pool_output,
                            q.output,
                        )),
                        None,
                    ),
                    Err(e) => (None, Some(e.to_string())),
                })
                .collect())
        }

        #[pyo3(signature = (rows, parallel=false))]
        fn clmm_intervals(
            &self,
            py: Python<'_>,
            rows: Vec<(u128, u128, u128)>,
            parallel: bool,
        ) -> PyResult<IntervalRows> {
            let results = py
                .detach(|| self.inner.interval_batch(rows, parallel))
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            Ok(results
                .into_iter()
                .map(|row| match row {
                    Ok(q) => (Some(q), None),
                    Err(e) => (None, Some(e.to_string())),
                })
                .collect())
        }
    }

    type PoolRow = (u64, u64, u32, u32, u8, u16, u64, u16, u64);
    type JobRow = (usize, bool, u64, u16);
    type SlimRows = Vec<(u64, u64, u8)>;
    type GridRows = Vec<(u64, u64, u64, u8)>;

    #[pyclass(frozen)]
    struct PoolBook {
        calculator: Arc<Calculator>,
        pools: Vec<CpmmPool>,
    }

    #[pymethods]
    impl PoolBook {
        #[new]
        fn new(engine: PyRef<'_, PoolMath>, rows: Vec<PoolRow>) -> PyResult<Self> {
            if rows.len() > MAX_BATCH {
                return Err(PyValueError::new_err("batch_too_large"));
            }
            let pools = rows
                .into_iter()
                .map(
                    |(a, b, trade, creator, fee_on, a_bps, a_max, b_bps, b_max)| {
                        CpmmPool {
                            reserve_a: RawAmount(a),
                            reserve_b: RawAmount(b),
                            trade_fee_rate: trade,
                            creator_fee_rate: creator,
                            fee_on,
                            transfer_a_bps: a_bps,
                            transfer_a_max: RawAmount(a_max),
                            transfer_b_bps: b_bps,
                            transfer_b_max: RawAmount(b_max),
                        }
                        .validate()
                    },
                )
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            Ok(Self {
                calculator: Arc::clone(&engine.inner),
                pools,
            })
        }

        #[pyo3(signature = (rows, parallel=false))]
        fn quote_batch(
            &self,
            py: Python<'_>,
            rows: Vec<JobRow>,
            parallel: bool,
        ) -> PyResult<SlimRows> {
            let jobs = rows
                .into_iter()
                .map(|(index, a_to_b, amount, slippage)| PoolJob {
                    index,
                    a_to_b,
                    amount,
                    slippage,
                })
                .collect();
            let results = py
                .detach(|| prepared_quotes(&self.calculator, &self.pools, jobs, parallel))
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            Ok(results
                .into_iter()
                .map(|r| match r {
                    Ok((out, minimum)) => (out, minimum, 0),
                    Err(e) => (0, 0, e.code()),
                })
                .collect())
        }

        #[pyo3(signature = (seeds, multipliers, slippage=50, parallel=false))]
        fn quote_grid(
            &self,
            py: Python<'_>,
            seeds: Vec<(usize, bool, u64)>,
            multipliers: Vec<u32>,
            slippage: u16,
            parallel: bool,
        ) -> PyResult<GridRows> {
            let results = py
                .detach(|| {
                    prepared_grid(
                        &self.calculator,
                        &self.pools,
                        seeds,
                        &multipliers,
                        slippage,
                        parallel,
                    )
                })
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            Ok(results
                .into_iter()
                .map(|r| match r {
                    Ok((amount, out, minimum)) => (amount, out, minimum, 0),
                    Err(e) => (0, 0, 0, e.code()),
                })
                .collect())
        }
    }

    #[pymodule]
    fn _pool_math(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<PoolMath>()?;
        m.add_class::<PoolBook>()?;
        m.add_function(wrap_pyfunction!(stock_values, m)?)?;
        m.add("STOCK_MONEY_MODEL", "stock-usdc-integer-v1")?;
        m.add("MAX_BATCH", MAX_BATCH)?;
        m.add("MODEL", "cp-swap-59fb845a-exact-in")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> CpmmInput {
        CpmmInput {
            amount: RawAmount(101),
            reserve_in: RawAmount(10000),
            reserve_out: RawAmount(20000),
            trade_fee_rate: 3000,
            creator_fee_rate: 10000,
            creator_on_input: true,
            input_transfer_fee: RawAmount(0),
            output_transfer_bps: 0,
            output_transfer_maximum: RawAmount(0),
        }
    }

    #[test]
    fn creator_combined_rounding_and_output_direction() -> Result<(), MathError> {
        let q = cpmm_exact_in(input())?;
        assert_eq!((q.trade_fee, q.creator_fee, q.output), (1, 1, 196));
        let q = cpmm_exact_in(CpmmInput {
            creator_on_input: false,
            ..input()
        })?;
        assert_eq!((q.trade_fee, q.creator_fee, q.output), (1, 2, 196));
        Ok(())
    }

    #[test]
    fn largest_amount_no_overflow_or_float() -> Result<(), MathError> {
        let mut i = input();
        i.amount = RawAmount(u64::MAX);
        i.reserve_in = RawAmount(u64::MAX);
        i.reserve_out = RawAmount(u64::MAX);
        i.trade_fee_rate = 0;
        i.creator_fee_rate = 0;
        assert_eq!(cpmm_exact_in(i)?.output, u64::MAX / 2);
        Ok(())
    }

    #[test]
    fn invalid_amount_fees_and_interval_fail() {
        assert_eq!(
            cpmm_exact_in(CpmmInput {
                amount: RawAmount(0),
                ..input()
            }),
            Err(MathError::InvalidAmount)
        );
        assert_eq!(
            cpmm_exact_in(CpmmInput {
                amount: RawAmount(1),
                ..input()
            }),
            Err(MathError::FeesConsumeInput)
        );
        assert_eq!(
            cpmm_exact_in(CpmmInput {
                trade_fee_rate: 999999,
                ..input()
            }),
            Err(MathError::InvalidFee)
        );
        assert_eq!(clmm_interval(1, 0, 1), Err(MathError::InvalidInterval));
        assert_eq!(clmm_interval(u128::MAX, 1, 2), Err(MathError::Overflow));
    }

    #[test]
    fn clmm_interval_exact_known_ratios() -> Result<(), MathError> {
        let q = 1_u128 << 64;
        assert_eq!(clmm_interval(100, 2 * q, q)?, (50, 100));
        assert_eq!(clmm_interval(100, q, 2 * q)?, (100, 50));
        Ok(())
    }

    #[test]
    fn rayon_order_and_partial_error_are_deterministic() -> Result<(), Box<dyn std::error::Error>> {
        let calc = Calculator::new(2)?;
        let mut rows = vec![input(); 2048];
        rows[7].amount = RawAmount(0);
        let sequential = calc.cpmm_batch(rows.clone(), false)?;
        assert_eq!(sequential, calc.cpmm_batch(rows, true)?);
        assert_eq!(sequential[7], Err(MathError::InvalidAmount));
        assert!(calc.cpmm_batch(vec![input(); MAX_BATCH + 1], true).is_err());
        assert!(Calculator::new(0).is_err());
        assert!(Calculator::new(17).is_err());
        Ok(())
    }

    #[test]
    fn prepared_grid_is_ordered_and_overflow_is_local() -> Result<(), Box<dyn std::error::Error>> {
        let calc = Calculator::new(4)?;
        let pool = CpmmPool {
            reserve_a: RawAmount(10000),
            reserve_b: RawAmount(20000),
            trade_fee_rate: 3000,
            creator_fee_rate: 10000,
            fee_on: 1,
            transfer_a_bps: 0,
            transfer_a_max: RawAmount(0),
            transfer_b_bps: 0,
            transfer_b_max: RawAmount(0),
        }
        .validate()?;
        let seeds = vec![(0, true, 101), (0, false, 103), (0, true, u64::MAX)];
        let seq = prepared_grid(&calc, &[pool], seeds.clone(), &[1, 2, 3], 50, false)?;
        assert_eq!(
            seq,
            prepared_grid(&calc, &[pool], seeds, &[1, 2, 3], 50, true)?
        );
        assert_eq!(seq[0]?, (101, 196, 195));
        assert_eq!(seq[7], Err(MathError::Overflow));
        assert_eq!(
            prepared_grid(
                &calc,
                &[pool],
                vec![(0, true, 1); MAX_BATCH],
                &[1, 2],
                50,
                true
            ),
            Err(MathError::BatchTooLarge)
        );
        assert_eq!(
            prepared_grid(&calc, &[pool], vec![(0, true, 1)], &[0], 50, true),
            Err(MathError::InvalidFee)
        );
        Ok(())
    }
}
