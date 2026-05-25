"""Unit tests for the three-format exit engine (spec execution_logic.html §3):

  * Format B — OHLCV  (trigger on the slot's own bar high/low)
  * Format C — LTP    (trigger collapses to the bar close)
  * Format A — Bid/Ask (LONG legs consult ASK, SHORT legs consult BID)

Covers the pure trigger-reference resolver, the bid/ask bar-type derivation,
and the config plumbing (ExitConfig → ManagedExitConfig).
"""

from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from core.models import ExitConfig, portfolio_from_dict, portfolio_to_dict
from core.managed_strategy import (
    _derive_bid_ask_bar_types,
    config_from_exit,
    resolve_trigger_hl,
)


# ── resolve_trigger_hl — Format B / C / A ───────────────────────────────────

def test_format_b_ohlcv_uses_bar_high_low():
    hi, lo = resolve_trigger_hl("ohlcv", True, False, close=1.10,
                                bar_high=1.12, bar_low=1.08)
    assert (hi, lo) == (1.12, 1.08)


def test_format_c_ltp_collapses_to_close():
    hi, lo = resolve_trigger_hl("ltp", False, True, close=1.10,
                                bar_high=1.12, bar_low=1.08)
    assert hi == 1.10 and lo == 1.10


def test_format_a_long_leg_uses_ask_series():
    # LONG leg → consults the ASK bar (SL on ask_low, TP on ask_high).
    hi, lo = resolve_trigger_hl(
        "bidask", True, False, close=1.10, bar_high=1.12, bar_low=1.08,
        bid_high=1.119, bid_low=1.079, ask_high=1.121, ask_low=1.081,
    )
    assert (hi, lo) == (1.121, 1.081)


def test_format_a_short_leg_uses_bid_series():
    # SHORT leg → consults the BID bar (SL on bid_high, TP on bid_low).
    hi, lo = resolve_trigger_hl(
        "bidask", False, True, close=1.10, bar_high=1.12, bar_low=1.08,
        bid_high=1.119, bid_low=1.079, ask_high=1.121, ask_low=1.081,
    )
    assert (hi, lo) == (1.119, 1.079)


def test_format_a_falls_back_to_ohlcv_when_pair_missing():
    # Data gap — no bid/ask for this ts → degrade to the slot's own OHLC.
    hi, lo = resolve_trigger_hl(
        "bidask", True, False, close=1.10, bar_high=1.12, bar_low=1.08,
        bid_high=None, bid_low=None, ask_high=None, ask_low=None,
    )
    assert (hi, lo) == (1.12, 1.08)


def test_mark_price_uses_previous_bar_close():
    # Mark Price (spec §3, approach B): trigger consults the PREVIOUS bar's
    # close (passed as mark_price), so the current bar's high/low (a wick) AND
    # its own close are ignored — high and low both = mark_price.
    hi, lo = resolve_trigger_hl("mark", False, True, close=1.10,
                                bar_high=1.12, bar_low=1.08, mark_price=1.095)
    assert hi == 1.095 and lo == 1.095


def test_mark_price_falls_back_to_close_on_first_bar():
    # No previous close yet (mark_price None/0) → fall back to the current close.
    hi, lo = resolve_trigger_hl("mark", True, False, close=1.10,
                                bar_high=1.12, bar_low=1.08, mark_price=None)
    assert hi == 1.10 and lo == 1.10
    hi2, lo2 = resolve_trigger_hl("mark", True, False, close=1.10,
                                  bar_high=1.12, bar_low=1.08, mark_price=0.0)
    assert hi2 == 1.10 and lo2 == 1.10


# ── _derive_bid_ask_bar_types ───────────────────────────────────────────────

def test_derive_bid_ask_from_mid():
    bid, ask = _derive_bid_ask_bar_types("EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL")
    assert bid == "EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL"
    assert ask == "EURUSD.FOREX_MS-1-MINUTE-ASK-EXTERNAL"


def test_derive_bid_ask_from_bid_primary():
    bid, ask = _derive_bid_ask_bar_types("EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL")
    assert bid == "EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL"
    assert ask == "EURUSD.FOREX_MS-1-MINUTE-ASK-EXTERNAL"


def test_derive_bid_ask_empty_for_last_bars():
    # Crypto LAST bars have no bid/ask pair.
    assert _derive_bid_ask_bar_types("BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL") == ("", "")


# ── config_from_exit — three-format threading ───────────────────────────────

def _managed_cfg(exit_config, bt_str):
    return config_from_exit(
        exit_config=exit_config, signal_name="EMA Cross", signal_params={},
        instrument_id=InstrumentId.from_str(bt_str.split("-")[0]),
        bar_type=BarType.from_str(bt_str), trade_size=1000,
    )


def test_config_threads_bidask_and_derives_pair():
    cfg = _managed_cfg(
        ExitConfig(exit_price_format="bidask"),
        "EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL",
    )
    assert cfg.exit_price_format == "bidask"
    assert cfg.bid_bar_type == "EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL"
    assert cfg.ask_bar_type == "EURUSD.FOREX_MS-1-MINUTE-ASK-EXTERNAL"


def test_config_bidask_falls_back_without_bid_ask_data():
    # A crypto LAST slot can't be Format A — degrades to ohlcv.
    cfg = _managed_cfg(
        ExitConfig(exit_price_format="bidask"),
        "BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL",
    )
    assert cfg.exit_price_format == "ohlcv"
    assert cfg.bid_bar_type == "" and cfg.ask_bar_type == ""


def test_config_ltp_threaded():
    cfg = _managed_cfg(
        ExitConfig(exit_price_format="ltp"),
        "BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL",
    )
    assert cfg.exit_price_format == "ltp"


def test_config_mark_passes_through():
    # "mark" passes config_from_exit unchanged; the crypto-only gate is enforced
    # later in on_start (needs a live venue), not here.
    cfg = _managed_cfg(
        ExitConfig(exit_price_format="mark"),
        "BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL",
    )
    assert cfg.exit_price_format == "mark"


def test_config_mark_case_insensitive():
    cfg = _managed_cfg(
        ExitConfig(exit_price_format="MARK"),
        "BTCUSD.CRYPTO-1-MINUTE-LAST-EXTERNAL",
    )
    assert cfg.exit_price_format == "mark"


def test_config_invalid_format_defaults_to_ohlcv():
    cfg = _managed_cfg(
        ExitConfig(exit_price_format="garbage"),
        "EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL",
    )
    assert cfg.exit_price_format == "ohlcv"


def test_config_default_is_ohlcv():
    cfg = _managed_cfg(ExitConfig(), "EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL")
    assert cfg.exit_price_format == "ohlcv"


# ── ExitConfig round-trip ───────────────────────────────────────────────────

def test_exit_price_format_roundtrip():
    pf = portfolio_from_dict({
        "name": "t",
        "slots": [{"strategy_name": "EMA Cross",
                   "exit_config": {"exit_price_format": "bidask"}}],
    })
    assert pf.slots[0].exit_config.exit_price_format == "bidask"
    assert portfolio_to_dict(pf)["slots"][0]["exit_config"]["exit_price_format"] == "bidask"
    # Old JSON without the field defaults to ohlcv.
    old = portfolio_from_dict({"name": "t2", "slots": [
        {"strategy_name": "EMA Cross", "exit_config": {}}]})
    assert old.slots[0].exit_config.exit_price_format == "ohlcv"
