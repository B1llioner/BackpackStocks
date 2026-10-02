import pytest
from solders.pubkey import Pubkey

from backpack_stocks.models import STOCK_MINT, USDC_MINT
from backpack_stocks.raydium import Raydium


class Pages:
    def __init__(self, pages):
        self.pages = pages
        self.requests = []

    async def request(self, method, path, **kwargs):
        self.requests.append(kwargs["params"])
        return self.pages[len(self.requests) - 1]


def row(address, a=STOCK_MINT, b=USDC_MINT):
    return {"id": address, "mintA": {"address": a}, "mintB": {"address": b}}


def page(rows, next_page=False):
    return {"success": True, "data": {"data": rows, "hasNextPage": next_page}}


async def test_pagination_dedup_actual_pair_filter_and_all_peers():
    addresses = [str(Pubkey.new_unique()) for _ in range(3)]
    other = str(Pubkey.new_unique())
    pages = Pages(
        [
            page([row(addresses[0]), row(addresses[1], b=other)], True),
            page([row(addresses[0]), row(addresses[2], a=other, b=USDC_MINT)]),
        ]
    )
    ray = Raydium(None, pages)
    result = await ray.pools()
    assert [p["id"] for p in result] == addresses[:1]
    assert [q["page"] for q in pages.requests] == [1, 2]
    pages = Pages([page([row(addresses[0]), row(addresses[1], b=other)])])
    result = await Raydium(None, pages).pools(STOCK_MINT, None)
    assert len(result) == 2 and "mint2" not in pages.requests[0]


@pytest.mark.parametrize("malformed", ["missing_flag", "failed", "oversized", "invalid_address"])
async def test_incomplete_or_invalid_catalog_not_reported_as_complete(malformed):
    response = page([])
    if malformed == "missing_flag":
        del response["data"]["hasNextPage"]
    elif malformed == "failed":
        response["success"] = False
    elif malformed == "oversized":
        response["data"]["data"] = [row(STOCK_MINT)] * 101
    else:
        response["data"]["data"] = [row("not-a-solana-address")]
    with pytest.raises(ValueError):
        await Raydium(None, Pages([response])).pools()


async def test_unbounded_pagination_fails_closed():
    pages = Pages([page([], True)] * 20)
    with pytest.raises(ValueError, match="bounded"):
        await Raydium(None, pages).pools()
    assert len(pages.requests) == 20
