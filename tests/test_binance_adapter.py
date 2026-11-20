"""Binance USD-M adapter: contract mapping, brackets, stop replacement, errors and the stream."""

from __future__ import annotations

import hashlib
import hmac
from decimal import Decimal
from urllib.parse import parse_qsl

import aiohttp
import pytest
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from signal_bot.binance.client import (
    BinanceAPIError,
    BinanceFuturesClient,
    SymbolRules,
    _fmt,
    parse_rules,
)
from signal_bot.config import Settings
from signal_bot.events import EventType
from signal_bot.execution.adapter import (
    AmbiguousOrderOutcome,
    AuthenticationFailed,
    FillPurpose,
    OrderRejected,
    OrderRequest,
    VenueOrderState,
)
from signal_bot.execution.binance_adapter import (
    BinanceFuturesAdapter,
    BinanceStreamBridge,
    normalize_ws_message,
)

URL_ = URL("https://demo-fapi.binance.com/fapi/v1/order")
INFO = aiohttp.RequestInfo(URL_, "POST", CIMultiDictProxy(CIMultiDict()), URL_)


def api_error(status: int, code: int | None, msg: str = "x") -> BinanceAPIError:
    return BinanceAPIError(INFO, (), status, code, msg)


RULES = {
    "SOLUSDT": SymbolRules(
        "SOLUSDT",
        Decimal("0.01"),
        Decimal("1"),
        Decimal("1"),
        Decimal("1000000"),
        Decimal("5000"),
        Decimal("5"),
    ),
    "BTCUSDT": SymbolRules(
        "BTCUSDT",
        Decimal("0.1"),
        Decimal("0.001"),
        Decimal("0.001"),
        Decimal("1000"),
        Decimal("120"),
        Decimal("100"),
    ),
}


class FakeClient:
    """In-memory stand-in for BinanceFuturesClient: records calls, holds positions/algos."""

    def __init__(self):
        self.rules = dict(RULES)
        self.calls: list[tuple[str, dict]] = []
        self.positions: dict[str, Decimal] = {}
        self.algos: list[dict] = []
        self.orders: dict[tuple[str, str], dict] = {}
        self.next_algo = 1
        self.fill_ratio = Decimal("1")
        self.fail_new_order: Exception | None = None
        self.fail_algo: Exception | None = None

    async def position_risk(self, symbol=None):
        self.calls.append(("position_risk", {"symbol": symbol}))
        return [
            {
                "symbol": s,
                "positionAmt": str(a),
                "entryPrice": "100",
                "unRealizedProfit": "1.5",
                "updateTime": 1_700_000_000_000,
            }
            for s, a in self.positions.items()
            if symbol in (None, s)
        ]

    async def open_algo_orders(self, symbol=None):
        self.calls.append(("open_algo_orders", {"symbol": symbol}))
        return [a for a in self.algos if symbol in (None, a["symbol"])]

    async def cancel_all_algo_orders(self, symbol):
        self.calls.append(("cancel_all_algo_orders", {"symbol": symbol}))
        self.algos = [a for a in self.algos if a["symbol"] != symbol]

    async def cancel_algo_order(self, client_algo_id=None, algo_id=None):
        self.calls.append(("cancel_algo_order", {"algo_id": algo_id}))
        self.algos = [a for a in self.algos if a["algoId"] != algo_id]
        return {"code": "200", "msg": "success"}

    async def new_algo_order(self, **p):
        self.calls.append(("new_algo_order", p))
        if self.fail_algo:
            raise self.fail_algo
        row = {
            "algoId": self.next_algo,
            "clientAlgoId": p["clientAlgoId"],
            "symbol": p["symbol"],
            "side": p["side"],
            "orderType": p["type"],
            "triggerPrice": str(p["triggerPrice"]),
            "algoStatus": "NEW",
            "quantity": str(p["quantity"]),
            "reduceOnly": True,
        }
        self.next_algo += 1
        self.algos.append(row)
        return row

    async def new_order(self, **p):
        self.calls.append(("new_order", p))
        if self.fail_new_order:
            raise self.fail_new_order
        qty = Decimal(str(p["quantity"]))
        filled = qty * self.fill_ratio
        sign = 1 if p["side"] == "BUY" else -1
        if p.get("reduceOnly"):
            self.positions[p["symbol"]] = Decimal("0")
        elif filled:
            self.positions[p["symbol"]] = sign * filled
        raw = {
            "orderId": 77,
            "clientOrderId": p["newClientOrderId"],
            "symbol": p["symbol"],
            "side": p["side"],
            "origQty": str(qty),
            "executedQty": str(filled),
            "avgPrice": "100.5" if filled else "0",
            "status": "FILLED" if filled == qty else "EXPIRED",
            "type": p["type"],
            "origType": p["type"],
            "reduceOnly": bool(p.get("reduceOnly")),
            "updateTime": 1_700_000_000_000,
        }
        self.orders[(p["symbol"], p["newClientOrderId"])] = raw
        return raw

    async def get_order(self, symbol, client_order_id=None, order_id=None):
        self.calls.append(("get_order", {"symbol": symbol}))
        raw = self.orders.get((symbol, client_order_id))
        if raw is None:
            raise api_error(400, -2013, "Order does not exist.")
        return raw

    async def multi_assets_mode(self):
        return getattr(self, "multi", False)

    async def set_single_asset_mode(self):
        self.calls.append(("set_single_asset_mode", {}))
        self.multi = False

    async def position_mode(self):
        return False

    async def user_trades(self, symbol, order_id=None, start_time_ms=None, limit=1000):
        return [{"qty": "2", "price": "100"}, {"qty": "3", "price": "101"}]

    async def account(self):
        return {"totalMarginBalance": "10012.5", "availableBalance": "9000"}


def adapter_with(fake: FakeClient) -> BinanceFuturesAdapter:
    return BinanceFuturesAdapter(fake)  # type: ignore[arg-type]


COID = "SB00" + "a" * 28


def request(**kw) -> OrderRequest:
    base = dict(
        client_order_id=COID,
        symbol="SOLUSDT",
        side="buy",
        size=Decimal("5"),
        order_type="limit_ioc",
        limit_price=Decimal("100.123"),
        stop_loss_price=Decimal("95.017"),
        take_profit_price=Decimal("107.5"),
    )
    base.update(kw)
    return OrderRequest(**base)


def names(fake):
    return [c[0] for c in fake.calls]


def test_instrument_is_one_step_per_contract():
    spec = adapter_with(FakeClient()).instrument("BTCUSDT")
    assert spec.contract_value == Decimal("0.001") and spec.lot_size == 1
    assert spec.tick_size == Decimal("0.1") and spec.max_contracts == 120_000


async def test_filled_entry_gets_stop_and_target_before_returning():
    fake = FakeClient()
    order = await adapter_with(fake).place_order(request())
    assert order.state == VenueOrderState.FILLED and order.filled_size == 5
    entry = next(p for n, p in fake.calls if n == "new_order")
    assert entry["type"] == "LIMIT" and entry["timeInForce"] == "IOC"
    assert entry["quantity"] == Decimal("5") and entry["price"] == Decimal("100.12")
    stop, target = [p for n, p in fake.calls if n == "new_algo_order"]
    assert stop["type"] == "STOP_MARKET" and stop["side"] == "SELL"
    assert stop["reduceOnly"] == "true" and "closePosition" not in stop
    assert stop["quantity"] == Decimal("5") and stop["triggerPrice"] == Decimal("95.02")
    assert stop["clientAlgoId"] == COID + "S0" and len(stop["clientAlgoId"]) <= 36
    assert target["type"] == "TAKE_PROFIT_MARKET" and target["clientAlgoId"] == COID + "T0"
    position = (await adapter_with(fake).positions())[0]
    assert position.has_stop_loss and position.has_take_profit
    assert position.size == 5 and position.stop_loss_price == Decimal("95.02")


async def test_amended_stop_covers_the_open_quantity():
    fake = FakeClient()
    adapter = adapter_with(fake)
    await adapter.place_order(request(take_profit_price=None))
    await adapter.amend_stop("SOLUSDT", COID, Decimal("97.5"))
    new = [p for n, p in fake.calls if n == "new_algo_order"][-1]
    assert new["reduceOnly"] == "true" and new["quantity"] == Decimal("5")


async def test_prepare_switches_off_multi_assets_mode():
    fake = FakeClient()
    fake.multi = True
    await adapter_with(fake).prepare()
    assert "set_single_asset_mode" in names(fake)


async def test_missing_fill_price_is_read_back_from_the_fills():
    fake = FakeClient()
    original = fake.new_order

    async def no_avg(**p):
        raw = await original(**p)
        raw["avgPrice"] = "0.0000"
        return raw

    fake.new_order = no_avg
    order = await adapter_with(fake).place_order(request())
    assert order.avg_fill_price == Decimal("100.6")  # (2*100 + 3*101) / 5


async def test_unfilled_ioc_attaches_nothing():
    fake = FakeClient()
    fake.fill_ratio = Decimal("0")
    order = await adapter_with(fake).place_order(request())
    assert order.state == VenueOrderState.EXPIRED and order.filled_size == 0
    assert "new_algo_order" not in names(fake)


async def test_leftover_conditionals_are_cleared_before_an_entry():
    fake = FakeClient()
    fake.algos.append(
        {
            "algoId": 9,
            "clientAlgoId": "oldS0",
            "symbol": "SOLUSDT",
            "side": "SELL",
            "orderType": "STOP_MARKET",
            "triggerPrice": "90",
            "algoStatus": "NEW",
        }
    )
    await adapter_with(fake).place_order(request())
    assert names(fake).index("cancel_all_algo_orders") < names(fake).index("new_order")


async def test_entry_refused_while_a_position_is_open_on_the_symbol():
    fake = FakeClient()
    fake.positions["SOLUSDT"] = Decimal("-3")
    with pytest.raises(OrderRejected, match="position_already_open"):
        await adapter_with(fake).place_order(request())
    assert "new_order" not in names(fake)


async def test_too_small_orders_are_refused_without_a_request():
    fake = FakeClient()
    with pytest.raises(OrderRejected, match="below_min_notional"):
        await adapter_with(fake).place_order(
            request(symbol="BTCUSDT", size=Decimal("1"), limit_price=Decimal("60000"))
        )
    assert fake.calls == []


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (api_error(503, None, "Unknown error"), AmbiguousOrderOutcome),
        (api_error(400, -1007, "Timeout waiting"), AmbiguousOrderOutcome),
        (api_error(408, None, "timeout"), AmbiguousOrderOutcome),
        (api_error(400, -2019, "Margin is insufficient."), OrderRejected),
        (api_error(401, -2015, "Invalid API-key"), AuthenticationFailed),
        (TimeoutError(), AmbiguousOrderOutcome),
    ],
)
async def test_placement_errors_are_classified(error, expected):
    fake = FakeClient()
    fake.fail_new_order = error
    with pytest.raises(expected):
        await adapter_with(fake).place_order(request())


async def test_amend_places_the_new_stop_before_cancelling_the_old():
    fake = FakeClient()
    adapter = adapter_with(fake)
    await adapter.place_order(request(take_profit_price=None))
    fake.calls.clear()
    position = await adapter.amend_stop("SOLUSDT", COID, Decimal("97.5"))
    new = next(p for n, p in fake.calls if n == "new_algo_order")
    assert new["clientAlgoId"] == COID + "S1" and new["triggerPrice"] == Decimal("97.50")
    assert names(fake).index("new_algo_order") < names(fake).index("cancel_algo_order")
    assert position.stop_loss_price == Decimal("97.50")
    assert [a["clientAlgoId"] for a in fake.algos] == [COID + "S1"]


async def test_a_refused_amend_leaves_the_old_stop():
    fake = FakeClient()
    adapter = adapter_with(fake)
    await adapter.place_order(request(take_profit_price=None))
    fake.fail_algo = api_error(400, -2021, "Order would immediately trigger.")
    with pytest.raises(OrderRejected, match="would_trigger"):
        await adapter.amend_stop("SOLUSDT", COID, Decimal("120"))
    assert len(fake.algos) == 1  # the original stop is still resting


async def test_get_order_searches_symbols_after_a_restart():
    fake = FakeClient()
    await adapter_with(fake).place_order(request())
    fresh = adapter_with(fake)  # no remembered symbols
    found = await fresh.get_order(COID)
    assert found is not None and found.symbol == "SOLUSDT" and found.filled_size == 5
    assert await fresh.get_order("SB00" + "f" * 28) is None


async def test_close_position_flattens_and_sweeps():
    fake = FakeClient()
    adapter = adapter_with(fake)
    await adapter.place_order(request())
    closed = await adapter.close_position("SOLUSDT", COID)
    assert closed.purpose == FillPurpose.CLOSE
    close = [p for n, p in fake.calls if n == "new_order"][-1]
    assert close["reduceOnly"] == "true" and close["side"] == "SELL"
    assert fake.algos == []


async def test_equity_is_margin_balance():
    adapter = adapter_with(FakeClient())
    assert await adapter.account_equity() == Decimal("10012.5")
    assert await adapter.available_balance() == Decimal("9000")


def test_stream_trade_and_position_events():
    step = {"SOLUSDT": Decimal("1"), "BTCUSDT": Decimal("0.001")}.__getitem__
    trade = {
        "e": "ORDER_TRADE_UPDATE",
        "E": 1,
        "T": 2,
        "o": {
            "s": "BTCUSDT",
            "c": "x1",
            "S": "SELL",
            "o": "MARKET",
            "ot": "STOP_MARKET",
            "q": "0.005",
            "z": "0.005",
            "ap": "61000",
            "X": "FILLED",
            "x": "TRADE",
            "i": 5,
            "l": "0.005",
            "L": "61000",
            "n": "0.12",
            "t": 99,
            "T": 1_700_000_000_000,
            "m": False,
            "R": True,
        },
    }
    fill, order = normalize_ws_message(trade, step)
    assert (
        fill.event_type == EventType.EXCHANGE_FILL
        and order.event_type == EventType.EXCHANGE_ORDER_UPDATE
    )
    assert fill.payload["purpose"] == "stop_loss" and Decimal(fill.payload["qty"]) == 5
    assert fill.payload["fill_id"] == "binance:BTCUSDT:99"
    account = {
        "e": "ACCOUNT_UPDATE",
        "E": 3,
        "T": 3,
        "a": {"m": "ORDER", "P": [{"s": "BTCUSDT", "pa": "-0.005", "ep": "61000", "up": "0"}]},
    }
    (pos,) = normalize_ws_message(account, step)
    assert (
        pos.event_type == EventType.EXCHANGE_POSITION_UPDATE and Decimal(pos.payload["size"]) == -5
    )


async def test_bridge_sweeps_a_symbol_that_went_flat(monkeypatch):
    import signal_bot.execution.binance_adapter as mod

    monkeypatch.setattr(mod, "FLAT_SWEEP_DELAY_SECONDS", 0)
    fake = FakeClient()
    fake.algos.append(
        {
            "algoId": 3,
            "clientAlgoId": "aT0",
            "symbol": "SOLUSDT",
            "side": "SELL",
            "orderType": "TAKE_PROFIT_MARKET",
            "triggerPrice": "110",
            "algoStatus": "NEW",
        }
    )

    class Bus:
        async def publish(self, event):
            pass

    bridge = BinanceStreamBridge(Bus(), adapter_with(fake))
    await bridge.handle_raw(
        {"e": "ACCOUNT_UPDATE", "T": 1, "a": {"P": [{"s": "SOLUSDT", "pa": "0", "ep": "0"}]}}
    )
    for task in list(bridge._sweeps):
        await task
    assert fake.algos == []


async def test_connect_publishes_the_open_positions():
    fake = FakeClient()
    adapter = adapter_with(fake)
    await adapter.place_order(request())  # 5 SOL long with its stop

    published = []

    class Bus:
        async def publish(self, event):
            published.append(event)

    await BinanceStreamBridge(Bus(), adapter).on_connect()
    positions = [e for e in published if e.event_type == EventType.EXCHANGE_POSITION_UPDATE]
    assert len(positions) == 1
    assert positions[0].payload["symbol"] == "SOLUSDT"
    assert Decimal(positions[0].payload["size"]) == 5
    assert positions[0].payload["has_stop_loss"] is True


# ---------------------------------------------------------------------------
# client helpers and configuration
# ---------------------------------------------------------------------------


def test_signed_query_is_hmac_of_the_parameters():
    client = BinanceFuturesClient(api_key="k", api_secret="s3cret", mode="demo")
    query = client.signed_query({"symbol": "BTCUSDT", "quantity": Decimal("0.00100")})
    body, signature = query.rsplit("&signature=", 1)
    assert dict(parse_qsl(body))["quantity"] == "0.001"
    assert signature == hmac.new(b"s3cret", body.encode(), hashlib.sha256).hexdigest()
    assert client.rest_url == "https://demo-fapi.binance.com"


def test_decimals_are_sent_without_exponents():
    assert _fmt(Decimal("1E-5")) == "0.00001" and _fmt(True) == "true"


def test_rules_keep_trading_perpetuals_only():
    info = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                    {
                        "filterType": "LOT_SIZE",
                        "stepSize": "0.001",
                        "minQty": "0.001",
                        "maxQty": "1000",
                    },
                    {"filterType": "MIN_NOTIONAL", "notional": "100"},
                ],
            },
            {
                "symbol": "ETHUSDT",
                "contractType": "CURRENT_QUARTER",
                "status": "TRADING",
                "filters": [],
            },
        ]
    }
    rules, missing = parse_rules(info, ["BTCUSDT", "ETHUSDT"])
    assert list(rules) == ["BTCUSDT"] and missing == ["ETHUSDT"]
    assert rules["BTCUSDT"].min_notional == 100 and rules["BTCUSDT"].tick_size == Decimal("0.10")


def test_binance_live_needs_keys_and_production_needs_acknowledgement():
    s = Settings(_env_file=None, exec_mode="live", exec_venue="binance")
    assert any("BINANCE_FUTURES_API_KEY" in p for p in s.validate_for_startup())
    demo = Settings(
        _env_file=None,
        exec_mode="live",
        exec_venue="binance",
        binance_futures_api_key="k",
        binance_futures_api_secret="s",
    )
    assert demo.validate_for_startup() == []
    prod = Settings(
        _env_file=None,
        exec_mode="live",
        exec_venue="binance",
        binance_trade_mode="production",
        binance_futures_api_key="k",
        binance_futures_api_secret="s",
    )
    assert any("LIVE_TRADING_ACKNOWLEDGED" in p for p in prod.validate_for_startup())
    secrets = Settings(_env_file=None, binance_futures_api_secret="hush").public_dict()
    assert "binance_futures_api_secret" not in secrets
