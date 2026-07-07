"""Tests for the strategy-level run_on_days gate (replaces the old pre-filter).

Pre-filtering bars by weekday before engine.add_data() created discontinuous
input that made Nautilus's internal TimeBarAggregator emit synthetic
stale-close bars across excluded days — on which composite-bar strategies
could fire phantom signals. The gate is now applied inside
ManagedExitStrategy._on_primary_bar so the bar stream stays continuous and
the aggregator never sees gaps.
"""

from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from core.backtest_runner import _allowed_weekdays
from core.managed_strategy import config_from_exit
from core.models import ExitConfig


# ── _allowed_weekdays resolution ────────────────────────────────────────────

def test_allowed_weekdays_none_passes_through():
    assert _allowed_weekdays(None) is None


def test_allowed_weekdays_named_days_resolve_to_ints():
    assert _allowed_weekdays(["MON", "TUE", "THU"]) == {0, 1, 3}
    assert _allowed_weekdays(["mon", "tue", "thu"]) == {0, 1, 3}
    assert _allowed_weekdays(["Monday", "Tuesday", "Thursday"]) == {0, 1, 3}


def test_allowed_weekdays_explicit_empty_means_no_days():
    # Distinct from None: caller has set the field but disabled every day.
    assert _allowed_weekdays([]) == set()


def test_allowed_weekdays_unknown_names_dropped():
    assert _allowed_weekdays(["MON", "Marsday", "THU"]) == {0, 3}


# ── config_from_exit forwards the gate value into ManagedExitConfig ─────────

_BAR = "EURUSD.SIM-1-MINUTE-MID-EXTERNAL"


def _cfg(allowed_weekdays):
    return config_from_exit(
        exit_config=ExitConfig(), signal_name="EMA Cross", signal_params={},
        instrument_id=InstrumentId.from_str("EURUSD.SIM"),
        bar_type=BarType.from_str(_BAR),
        trade_size=1000,
        allowed_weekdays=allowed_weekdays,
    )


def test_config_default_is_none_meaning_no_filter():
    # Omitted entirely → field stays at its default sentinel ``None``.
    cfg = config_from_exit(
        exit_config=ExitConfig(), signal_name="EMA Cross", signal_params={},
        instrument_id=InstrumentId.from_str("EURUSD.SIM"),
        bar_type=BarType.from_str(_BAR),
        trade_size=1000,
    )
    assert cfg.allowed_weekdays is None


def test_config_explicit_none_is_no_filter():
    cfg = _cfg(None)
    assert cfg.allowed_weekdays is None


def test_config_list_of_ints_passes_through():
    cfg = _cfg([0, 1, 3])
    assert cfg.allowed_weekdays == [0, 1, 3]


def test_config_empty_list_distinguishes_from_none():
    # ``[]`` means "no weekdays allowed" — strategy will block every entry.
    # ``None`` means "no filter — every day allowed". They must NOT collapse.
    cfg = _cfg([])
    assert cfg.allowed_weekdays == []
    assert cfg.allowed_weekdays is not None
