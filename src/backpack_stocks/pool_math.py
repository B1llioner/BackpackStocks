"""Integer pool math. Only supported state produces an amount quote.

Spot prices are decimal strings; on-chain amounts and fee rounding are integers.
CLMM spot must never be used as an amount quote across tick boundaries.
"""

from __future__ import annotations

from decimal import Decimal, localcontext

FEE_DENOMINATOR = 1_000_000
Q64 = 2**64


def ceil_div(value: int, divisor: int) -> int:
    if value < 0 or divisor <= 0:
        raise ValueError("Invalid nonnegative division")
    return (value + divisor - 1) // divisor


def spot_b_per_a(reserve_a: int, reserve_b: int, decimals_a: int, decimals_b: int) -> str:
    if (
        min(reserve_a, reserve_b) <= 0
        or not 0 <= min(decimals_a, decimals_b) <= max(decimals_a, decimals_b) <= 255
    ):
        raise ValueError("Invalid reserves or precision")
    with localcontext() as ctx:
        ctx.prec = 80
        return str(Decimal(reserve_b) / Decimal(reserve_a) * Decimal(10) ** (decimals_a - decimals_b))


def cpmm_exact_in(
    amount: int,
    reserve_in: int,
    reserve_out: int,
    trade_fee_rate: int,
    *,
    creator_fee_rate: int = 0,
    creator_on_input: bool = True,
    input_transfer_fee: int = 0,
    output_transfer_bps: int = 0,
    output_transfer_maximum: int = 0,
) -> dict[str, int]:
    if (
        any(
            type(x) is not int
            for x in (
                amount,
                reserve_in,
                reserve_out,
                trade_fee_rate,
                creator_fee_rate,
                input_transfer_fee,
                output_transfer_bps,
                output_transfer_maximum,
            )
        )
        or type(creator_on_input) is not bool
    ):
        raise ValueError("Amounts and rates must be integers")
    if min(amount, reserve_in, reserve_out) <= 0 or max(amount, reserve_in, reserve_out) >= 2**64:
        raise ValueError("Invalid amount or empty pool")
    if not 0 <= trade_fee_rate < FEE_DENOMINATOR or not 0 <= creator_fee_rate < FEE_DENOMINATOR:
        raise ValueError("Invalid CPMM fee rate")
    if creator_on_input and trade_fee_rate + creator_fee_rate >= FEE_DENOMINATOR:
        raise ValueError("Combined fee consumes input")
    if (
        not 0 <= input_transfer_fee < amount
        or not 0 <= output_transfer_bps <= 10000
        or output_transfer_maximum < 0
    ):
        raise ValueError("Invalid transfer fee")
    received = amount - input_transfer_fee
    # cp-swap 59fb845a: ceil the combined input fee, then floor its creator share.
    # Separate ceilings (as in the older SDK) can overcharge one raw token unit.
    if creator_on_input:
        rate = trade_fee_rate + creator_fee_rate
        total_fee = ceil_div(received * rate, FEE_DENOMINATOR)
        creator_fee = total_fee * creator_fee_rate // rate if rate else 0
        trade_fee = total_fee - creator_fee
    else:
        trade_fee = ceil_div(received * trade_fee_rate, FEE_DENOMINATOR)
        creator_fee = 0
    net = received - trade_fee - creator_fee
    if net <= 0:
        raise ValueError("Fees consume input")
    gross_out = net * reserve_out // (reserve_in + net)
    if not creator_on_input:
        creator_fee = ceil_div(gross_out * creator_fee_rate, FEE_DENOMINATOR)
    before_transfer = gross_out - (0 if creator_on_input else creator_fee)
    transfer_out = min(output_transfer_maximum, ceil_div(before_transfer * output_transfer_bps, 10000))
    output = before_transfer - transfer_out
    if output <= 0:
        raise ValueError("Amount rounds to zero output")
    return {
        "input_raw": amount,
        "received_raw": received,
        "trade_fee_raw": trade_fee,
        "creator_fee_raw": creator_fee,
        "input_transfer_fee_raw": input_transfer_fee,
        "output_transfer_fee_raw": transfer_out,
        "pool_output_raw": gross_out,
        "output_raw": output,
    }


def clmm_interval_amounts(liquidity: int, sqrt_start: int, sqrt_end: int) -> dict[str, int]:
    """Fee-free movement inside ONE interval with constant active liquidity.

    Caller must prove the interval has no initialized ticks/limit orders. This
    primitive alone is never a full CLMM quote.
    """
    if min(liquidity, sqrt_start, sqrt_end) <= 0 or sqrt_start == sqrt_end:
        raise ValueError("Invalid CLMM interval")
    low, high = sorted((sqrt_start, sqrt_end))
    numerator_a = liquidity * Q64 * (high - low)
    numerator_b = liquidity * (high - low)
    if sqrt_end < sqrt_start:
        return {"input_a": ceil_div(numerator_a, high * low), "output_b": numerator_b // Q64}
    return {"input_b": ceil_div(numerator_b, Q64), "output_a": numerator_a // (high * low)}
