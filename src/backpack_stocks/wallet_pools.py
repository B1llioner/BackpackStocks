"""Recognize public Raydium swap instructions; pool hints never become prices."""

from __future__ import annotations

import hashlib

from solders.pubkey import Pubkey

from .pool_state import CLMM, CPMM

ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def instruction_bytes(value: str) -> bytes:
    if not isinstance(value, str) or not 1 <= len(value) <= 128:
        raise ValueError("Unbounded swap instruction data")
    number = 0
    for character in value:
        number = number * 58 + ALPHABET.index(character)
    zeros = len(value) - len(value.lstrip("1"))
    return bytes(zeros) + number.to_bytes((number.bit_length() + 7) // 8, "big")


def swap_pool_hints(instructions: list[dict]) -> list[dict]:
    result = {}
    for instruction in instructions:
        program = instruction.get("programId")
        if program not in {CLMM, CPMM}:
            continue
        try:
            data = instruction_bytes(instruction.get("data", ""))
            names = ["swap_v2"] if program == CLMM else ["swap_base_input", "swap_base_output"]
            if data[:8] not in {hashlib.sha256(("global:" + name).encode()).digest()[:8] for name in names}:
                continue
            accounts = instruction["accounts"]
            if len(data) != (41 if program == CLMM else 24) or len(accounts) < 13:
                continue
            index = 2 if program == CLMM else 3
            address = str(Pubkey.from_string(accounts[index]))
            result[address] = {"pool": address, "program": program}
        except (ValueError, KeyError, TypeError):
            continue
    return list(result.values())[:24]
