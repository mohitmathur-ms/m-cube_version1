"""
Verification script for changes made this session.

Exercises:
1. _allowed_weekdays() helper -- all input shapes
2. _filter_bars_by_weekday() helper -- math correctness
3. PortfolioConfig run_on_days schema -- default, round-trip, JSON
4. Bug fix in _run_slot_group -- bars_load block uses .extend (not nested .append)
5. Wiring -- _run_single_slot and _run_slot_group accept default_run_on_days
6. run_portfolio_backtest -- passes portfolio.run_on_days to workers
7. UI translation logic in portfolio.js -- string parse correctness

Run from project root: python verify_session_changes.py
Exit code 0 = all checks passed; 1 = at least one failure.
"""
from __future__ import annotations

import inspect
import json
import re
import sys
from dataclasses import asdict
from pathlib import Path


# ANSI color codes (work on Windows 10+ terminals; fall through harmlessly otherwise)
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"

failures: list[str] = []


def check(label: str, cond: bool, detail: str = ""):
    """Record a single check."""
    if cond:
        print(f"  {GREEN}PASS{RESET}  {label}")
    else:
        print(f"  {RED}FAIL{RESET}  {label}" + (f"  ({detail})" if detail else ""))
        failures.append(label)


def section(name: str):
    print(f"\n{YELLOW}=== {name} ==={RESET}")


# ─────────────────────────────────────────────────────────────────────────────
# 1.  _allowed_weekdays helper
# ─────────────────────────────────────────────────────────────────────────────
section("1.  _allowed_weekdays() -- input shape handling")

from core.backtest_runner import _allowed_weekdays  # noqa: E402

check("None input -> None (no filter)",
      _allowed_weekdays(None) is None)
check("['Mon'] -> {0}",
      _allowed_weekdays(["Mon"]) == {0})
check("['Mon','Tue','Wed','Thu','Fri'] -> {0,1,2,3,4}",
      _allowed_weekdays(["Mon", "Tue", "Wed", "Thu", "Fri"]) == {0, 1, 2, 3, 4})
check("Case-insensitive: ['mon','TUESDAY'] -> {0,1}",
      _allowed_weekdays(["mon", "TUESDAY"]) == {0, 1})
check("Full names: ['Sunday'] -> {6}",
      _allowed_weekdays(["Sunday"]) == {6})
check("Mixed valid/invalid: ['Mon','BOGUS','Tue'] -> {0,1}",
      _allowed_weekdays(["Mon", "BOGUS", "Tue"]) == {0, 1})
check("Empty list -> empty set (all days blocked)",
      _allowed_weekdays([]) == set())
check("All invalid -> empty set (all days blocked)",
      _allowed_weekdays(["BOGUS", "OTHER"]) == set())
check("Non-list non-None (e.g. string) -> None (treated as no filter)",
      _allowed_weekdays("Mon") is None)
check("Whitespace tolerated: ['  Mon  '] -> {0}",
      _allowed_weekdays(["  Mon  "]) == {0})


# ─────────────────────────────────────────────────────────────────────────────
# 2.  _filter_bars_by_weekday helper
# ─────────────────────────────────────────────────────────────────────────────
section("2.  _filter_bars_by_weekday() -- math correctness")

from core.backtest_runner import _filter_bars_by_weekday  # noqa: E402


class FakeBar:
    """Stand-in for nautilus_trader Bar -- only ts_event is touched."""
    def __init__(self, ts_event: int):
        self.ts_event = ts_event


# 1970-01-01 = Thursday (weekday=3). 1970-01-05 = Monday.
NANOS_PER_DAY = 86_400_000_000_000
THU_19700101 = 0
FRI_19700102 = 1 * NANOS_PER_DAY
SAT_19700103 = 2 * NANOS_PER_DAY
SUN_19700104 = 3 * NANOS_PER_DAY
MON_19700105 = 4 * NANOS_PER_DAY
TUE_19700106 = 5 * NANOS_PER_DAY
WED_19700107 = 6 * NANOS_PER_DAY
THU_19700108 = 7 * NANOS_PER_DAY  # one week later

bars = [
    FakeBar(THU_19700101),
    FakeBar(FRI_19700102),
    FakeBar(SAT_19700103),
    FakeBar(SUN_19700104),
    FakeBar(MON_19700105),
    FakeBar(TUE_19700106),
    FakeBar(WED_19700107),
    FakeBar(THU_19700108),
]

# None -> no filter
kept, dropped = _filter_bars_by_weekday(bars, None)
check("allowed=None -> returns input unchanged, dropped=0",
      len(kept) == 8 and dropped == 0)

# Empty set -> drop everything
kept, dropped = _filter_bars_by_weekday(bars, set())
check("allowed=set() -> drops all 8 bars",
      len(kept) == 0 and dropped == 8)

# Just Monday {0}
kept, dropped = _filter_bars_by_weekday(bars, {0})
check("allowed={Mon} -> keeps 1 (the Mon bar), drops 7",
      len(kept) == 1 and dropped == 7
      and kept[0].ts_event == MON_19700105)

# Just Thursday {3} -- should match both Thursdays in our test set
kept, dropped = _filter_bars_by_weekday(bars, {3})
check("allowed={Thu} -> keeps 2 (both Thursdays), drops 6",
      len(kept) == 2 and dropped == 6
      and kept[0].ts_event == THU_19700101
      and kept[1].ts_event == THU_19700108)

# Weekdays only {0,1,2,3,4} -- drops Sat+Sun
kept, dropped = _filter_bars_by_weekday(bars, {0, 1, 2, 3, 4})
check("allowed=Mon-Fri -> keeps 6, drops Sat+Sun (2)",
      len(kept) == 6 and dropped == 2)

# Empty input
kept, dropped = _filter_bars_by_weekday([], {0})
check("empty input bars -> empty output, dropped=0",
      kept == [] and dropped == 0)

# Sub-day (intra-day) timestamps still resolve correctly
intra_day_mon = MON_19700105 + 12 * 3_600_000_000_000  # noon Monday
kept, dropped = _filter_bars_by_weekday([FakeBar(intra_day_mon)], {0})
check("intra-day Mon ts -> counted as Monday",
      len(kept) == 1 and dropped == 0)

# Cross-check against pandas (independent oracle)
try:
    import pandas as pd
    test_ts_list = [
        ("2024-01-01 09:30:00", 0),  # Mon
        ("2024-01-02 09:30:00", 1),  # Tue
        ("2024-01-03 09:30:00", 2),  # Wed
        ("2024-01-04 09:30:00", 3),  # Thu
        ("2024-01-05 09:30:00", 4),  # Fri
        ("2024-01-06 09:30:00", 5),  # Sat
        ("2024-01-07 09:30:00", 6),  # Sun
    ]
    all_match = True
    for ts_str, expected_wd in test_ts_list:
        ts_ns = pd.Timestamp(ts_str, tz="UTC").value  # nanoseconds
        kept, _ = _filter_bars_by_weekday([FakeBar(ts_ns)], {expected_wd})
        if len(kept) != 1:
            all_match = False
            break
    check("integer-modulo weekday matches pandas Timestamp.weekday() across Mon-Sun",
          all_match)
except ImportError:
    check("pandas oracle skipped (not installed)", True)


# ─────────────────────────────────────────────────────────────────────────────
# 3.  PortfolioConfig schema -- round-trip with run_on_days
# ─────────────────────────────────────────────────────────────────────────────
section("3.  PortfolioConfig.run_on_days schema")

from core.models import (  # noqa: E402
    PortfolioConfig,
    StrategySlotConfig,
    portfolio_from_dict,
    portfolio_to_dict,
)

cfg = PortfolioConfig(name="Test")
check("Default run_on_days is None",
      cfg.run_on_days is None)

cfg.run_on_days = ["Mon", "Tue", "Wed"]
d = portfolio_to_dict(cfg)
check("portfolio_to_dict serializes run_on_days",
      d.get("run_on_days") == ["Mon", "Tue", "Wed"])

# Round-trip through dict
cfg2 = portfolio_from_dict(d)
check("portfolio_from_dict round-trips run_on_days",
      cfg2.run_on_days == ["Mon", "Tue", "Wed"])

# JSON round-trip
js = json.dumps(d)
cfg3 = portfolio_from_dict(json.loads(js))
check("Full JSON round-trip preserves run_on_days",
      cfg3.run_on_days == ["Mon", "Tue", "Wed"])

# Loading old portfolio JSON without run_on_days field still works
old_data = {
    "name": "Legacy",
    "description": "",
    "starting_capital": 100_000.0,
    "allocation_mode": "equal",
    "slots": [],
    "created_at": "2024-01-01T00:00:00+00:00",
    "updated_at": "2024-01-01T00:00:00+00:00",
}
try:
    legacy = portfolio_from_dict(old_data)
    check("Legacy portfolio JSON (without run_on_days field) loads with default None",
          legacy.run_on_days is None)
except TypeError as e:
    check("Legacy portfolio JSON loads without error",
          False, f"TypeError: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 4.  Bug fix verification -- _run_slot_group uses extend, not nested append
# ─────────────────────────────────────────────────────────────────────────────
section("4.  Bug fix: _run_slot_group bars_load uses .extend (not append+nesting)")

src = Path("core/backtest_runner.py").read_text(encoding="utf-8")

# The old buggy line was:  bar_type_strs_to_load.append(pair)
# The fix changed it to:   bar_type_strs_to_load.extend(paired_strs)
check("Buggy '.append(pair)' (where pair is a list) is gone",
      "bar_type_strs_to_load.append(pair)" not in src)
check("Buggy 'if pair is not None:' is gone",
      "if pair is not None" not in src)
check("Fixed '.extend(paired_strs)' is present in _run_slot_group context",
      "paired_strs = _pair_bid_ask_bar_type(primary_bar_type_str)" in src
      and "bar_type_strs_to_load.extend(paired_strs)" in src)
check("missing_pairs tracking added",
      src.count("missing_pairs.append(bt_str)") >= 4)  # 2 sites x 2 places each
check("Warning surfacing in _run_slot_group result loop",
      'if missing_pairs:' in src and 'r["warning"]' in src)


# ─────────────────────────────────────────────────────────────────────────────
# 5.  Wiring verification -- function signatures accept default_run_on_days
# ─────────────────────────────────────────────────────────────────────────────
section("5.  Wiring: _run_single_slot / _run_slot_group accept default_run_on_days")

from core.backtest_runner import _run_single_slot, _run_slot_group  # noqa: E402

sig_single = inspect.signature(_run_single_slot)
check("_run_single_slot has default_run_on_days parameter",
      "default_run_on_days" in sig_single.parameters)
check("_run_single_slot.default_run_on_days defaults to None",
      sig_single.parameters.get("default_run_on_days").default is None
      if "default_run_on_days" in sig_single.parameters else False)

sig_group = inspect.signature(_run_slot_group)
check("_run_slot_group has default_run_on_days parameter",
      "default_run_on_days" in sig_group.parameters)
check("_run_slot_group.default_run_on_days defaults to None",
      sig_group.parameters.get("default_run_on_days").default is None
      if "default_run_on_days" in sig_group.parameters else False)


# ─────────────────────────────────────────────────────────────────────────────
# 6.  run_portfolio_backtest passes portfolio.run_on_days down
# ─────────────────────────────────────────────────────────────────────────────
section("6.  run_portfolio_backtest pipes portfolio.run_on_days to workers")

# Count occurrences in the source -- should appear once per submit() site.
# Sites: 3 in run_portfolio_backtest (single in grouping, group in grouping, single in non-grouping).
matches = re.findall(r"default_run_on_days=portfolio\.run_on_days", src)
check("'default_run_on_days=portfolio.run_on_days' appears at all 3 submit sites",
      len(matches) == 3,
      f"found {len(matches)}, expected 3")


# ─────────────────────────────────────────────────────────────────────────────
# 7.  UI translation logic mirrors backend expectations
# ─────────────────────────────────────────────────────────────────────────────
section("7.  portfolio.js UI translation: dropdown -> backend list shape")

js_src = Path("static/js/portfolio.js").read_text(encoding="utf-8")

check("UI saves to pf.run_on_days (not just pf._ui.run_on_days)",
      'pf.run_on_days = ' in js_src)
check("Custom branch reads from pf._ui.selected_days",
      'pf._ui.selected_days' in js_src
      and 'pf.run_on_days = pf._ui.selected_days' in js_src)
check("Mon - Fri preset expands to 5-day list",
      '["Mon","Tue","Wed","Thu","Fri"]' in js_src)
check("Mon - Thu preset expands to 4-day list",
      '["Mon","Tue","Wed","Thu"]' in js_src)
check("'All Days' branch sets run_on_days to null (no filter)",
      'pf.run_on_days = null' in js_src)

# Verify the day strings the UI sends are accepted by the backend helper
ui_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
backend_resolved = _allowed_weekdays(ui_days)
check("UI day strings (Mon, Tue, ...) are accepted by _allowed_weekdays",
      backend_resolved == {0, 1, 2, 3, 4, 5, 6})


# ─────────────────────────────────────────────────────────────────────────────
# 8.  End-to-end happy path simulation (no actual engine, just data flow)
# ─────────────────────────────────────────────────────────────────────────────
section("8.  End-to-end data flow simulation")

# Build a portfolio JSON the way the UI would produce it
ui_portfolio = {
    "name": "Smoke Test",
    "description": "",
    "starting_capital": 100_000.0,
    "allocation_mode": "equal",
    "run_on_days": ["Mon", "Tue", "Wed", "Thu", "Fri"],
    "slots": [],
    "created_at": "2024-01-01T00:00:00+00:00",
    "updated_at": "2024-01-01T00:00:00+00:00",
}

p = portfolio_from_dict(ui_portfolio)
check("UI-shaped portfolio loads with run_on_days intact",
      p.run_on_days == ["Mon", "Tue", "Wed", "Thu", "Fri"])

# Resolve as the worker would
allowed = _allowed_weekdays(p.run_on_days)
check("Portfolio.run_on_days resolves to {0,1,2,3,4} via worker helper",
      allowed == {0, 1, 2, 3, 4})

# Filter a representative week of bars
week_bars = [FakeBar(i * NANOS_PER_DAY) for i in range(3, 10)]  # Thu-Wed
kept, dropped = _filter_bars_by_weekday(week_bars, allowed)
# Days in this range: Thu, Fri, Sat, Sun, Mon, Tue, Wed -> 5 weekdays kept, 2 dropped
check("Filter keeps 5 weekdays of 7-day span, drops 2 (Sat+Sun)",
      len(kept) == 5 and dropped == 2)


# ─────────────────────────────────────────────────────────────────────────────
# 9.  _hhmm_to_minute helper
# ─────────────────────────────────────────────────────────────────────────────
section("9.  _hhmm_to_minute() -- input parsing")

from core.backtest_runner import _hhmm_to_minute  # noqa: E402

check("None -> None",
      _hhmm_to_minute(None) is None)
check("Empty string -> None",
      _hhmm_to_minute("") is None)
check("'09:30' -> 570 (= 9*60+30)",
      _hhmm_to_minute("09:30") == 570)
check("'09:30:45' (HH:MM:SS) -> 570 (seconds ignored)",
      _hhmm_to_minute("09:30:45") == 570)
check("'00:00' -> 0",
      _hhmm_to_minute("00:00") == 0)
check("'23:59' -> 1439",
      _hhmm_to_minute("23:59") == 1439)
check("Whitespace tolerated: ' 09:30 '",
      _hhmm_to_minute(" 09:30 ") == 570)
check("Out-of-range hour 25:00 -> None",
      _hhmm_to_minute("25:00") is None)
check("Out-of-range minute 09:99 -> None",
      _hhmm_to_minute("09:99") is None)
check("Garbage 'abc' -> None",
      _hhmm_to_minute("abc") is None)


# ─────────────────────────────────────────────────────────────────────────────
# 10. _filter_bars_by_time_of_day helper
# ─────────────────────────────────────────────────────────────────────────────
section("10. _filter_bars_by_time_of_day() -- intra-day window")

from core.backtest_runner import _filter_bars_by_time_of_day  # noqa: E402

# Build a single Monday's worth of bars at every minute in [00:00, 23:59].
# Monday = 4 days after epoch. Each bar at MON_19700105 + minute*60s.
day_bars = []
for h in range(24):
    for m in range(60):
        ts = MON_19700105 + (h * 60 + m) * 60_000_000_000
        day_bars.append(FakeBar(ts))

# Both endpoints None -> no filter
kept, dropped = _filter_bars_by_time_of_day(day_bars, None, None)
check("Both endpoints None -> input unchanged",
      len(kept) == 24 * 60 and dropped == 0)

# Only start -> end is unbounded (23:59)
kept, dropped = _filter_bars_by_time_of_day(day_bars, "12:00", None)
check("start='12:00', end=None -> keeps half-day from noon (720 bars)",
      len(kept) == 720 and dropped == 720)

# Only end -> start is unbounded (00:00)
kept, dropped = _filter_bars_by_time_of_day(day_bars, None, "11:59")
check("start=None, end='11:59' -> keeps before-noon (720 bars)",
      len(kept) == 720 and dropped == 720)

# Both endpoints same minute -> keeps just that minute
kept, dropped = _filter_bars_by_time_of_day(day_bars, "09:30", "09:30")
check("start=end='09:30' -> keeps exactly 1 bar (09:30)",
      len(kept) == 1 and dropped == 1439)

# Standard equity-market window 09:30..16:00 inclusive -> 391 minutes
kept, dropped = _filter_bars_by_time_of_day(day_bars, "09:30", "16:00")
check("'09:30'..'16:00' inclusive -> 391 bars (390 + 1)",
      len(kept) == 391 and dropped == 1440 - 391)

# Wrap-around window: 22:00 to 02:00 -> keeps [22:00..23:59] + [00:00..02:00]
kept, dropped = _filter_bars_by_time_of_day(day_bars, "22:00", "02:00")
expected = (24 * 60 - 22 * 60) + (2 * 60 + 1)  # [22:00,24:00) + [00:00,02:00] inclusive
check("Wrap-around '22:00'..'02:00' -> keeps overnight window correctly",
      len(kept) == expected,
      f"got {len(kept)}, expected {expected}")

# Bad input pair (both invalid) -> input unchanged (both endpoints resolve to None)
kept, dropped = _filter_bars_by_time_of_day(day_bars, "abc", "xyz")
check("Garbage endpoints both None -> no filter applied",
      len(kept) == 24 * 60 and dropped == 0)

# Mix: valid start, garbage end -> end falls through to 23:59 (unbounded)
kept, dropped = _filter_bars_by_time_of_day(day_bars, "09:30", "garbage")
check("Valid start + garbage end -> garbage end treated as unbounded",
      len(kept) == 24 * 60 - 9 * 60 - 30 and dropped == 9 * 60 + 30)


# ─────────────────────────────────────────────────────────────────────────────
# 11. PortfolioConfig.entry_start_time / entry_end_time schema
# ─────────────────────────────────────────────────────────────────────────────
section("11. PortfolioConfig entry-window schema")

cfg = PortfolioConfig(name="WindowTest")
check("Default entry_start_time is None",
      cfg.entry_start_time is None)
check("Default entry_end_time is None",
      cfg.entry_end_time is None)

cfg.entry_start_time = "09:30"
cfg.entry_end_time = "16:00"
d = portfolio_to_dict(cfg)
check("entry_start_time and entry_end_time serialize to dict",
      d.get("entry_start_time") == "09:30" and d.get("entry_end_time") == "16:00")

cfg2 = portfolio_from_dict(d)
check("Round-trip preserves entry_start_time and entry_end_time",
      cfg2.entry_start_time == "09:30" and cfg2.entry_end_time == "16:00")

# Legacy JSON without these fields still loads
legacy = portfolio_from_dict({
    "name": "Legacy", "description": "", "starting_capital": 100_000.0,
    "allocation_mode": "equal", "slots": [],
    "created_at": "2024-01-01T00:00:00+00:00",
    "updated_at": "2024-01-01T00:00:00+00:00",
})
check("Legacy JSON without entry-window fields loads with defaults None",
      legacy.entry_start_time is None and legacy.entry_end_time is None)


# ─────────────────────────────────────────────────────────────────────────────
# 12. Wiring -- worker functions accept entry_start_time / entry_end_time
# ─────────────────────────────────────────────────────────────────────────────
section("12. Worker functions accept default_entry_start_time / default_entry_end_time")

sig_single = inspect.signature(_run_single_slot)
check("_run_single_slot has default_entry_start_time parameter",
      "default_entry_start_time" in sig_single.parameters)
check("_run_single_slot has default_entry_end_time parameter",
      "default_entry_end_time" in sig_single.parameters)
check("Both default to None",
      sig_single.parameters.get("default_entry_start_time").default is None
      and sig_single.parameters.get("default_entry_end_time").default is None)

sig_group = inspect.signature(_run_slot_group)
check("_run_slot_group has default_entry_start_time parameter",
      "default_entry_start_time" in sig_group.parameters)
check("_run_slot_group has default_entry_end_time parameter",
      "default_entry_end_time" in sig_group.parameters)


# ─────────────────────────────────────────────────────────────────────────────
# 13. run_portfolio_backtest pipes both entry-window fields
# ─────────────────────────────────────────────────────────────────────────────
section("13. run_portfolio_backtest pipes entry_start_time / entry_end_time")

start_matches = re.findall(r"default_entry_start_time=portfolio\.entry_start_time", src)
end_matches = re.findall(r"default_entry_end_time=portfolio\.entry_end_time", src)
check("'default_entry_start_time=portfolio.entry_start_time' at all 3 submit sites",
      len(start_matches) == 3,
      f"found {len(start_matches)}, expected 3")
check("'default_entry_end_time=portfolio.entry_end_time' at all 3 submit sites",
      len(end_matches) == 3,
      f"found {len(end_matches)}, expected 3")


# ─────────────────────────────────────────────────────────────────────────────
# 14. UI translation -- start_time / end_time
# ─────────────────────────────────────────────────────────────────────────────
section("14. portfolio.js wires Start Time / End Time to backend")

check("UI saves to pf.entry_start_time",
      'pf.entry_start_time = ' in js_src)
check("UI saves to pf.entry_end_time",
      'pf.entry_end_time = ' in js_src)
check("Empty/sentinel start time -> null",
      'pf.entry_start_time = pf._ui.start_time && pf._ui.start_time !== "00:00:00"' in js_src)


# ─────────────────────────────────────────────────────────────────────────────
# 15. CSS -- pf-live-only class exists with distinct styling
# ─────────────────────────────────────────────────────────────────────────────
section("15. style.css -- pf-live-only class added")

css_src = Path("static/css/style.css").read_text(encoding="utf-8")
check(".pf-live-only base rule exists",
      ".pf-live-only" in css_src and "background: #f5f5f5" in css_src)
check(".pf-live-only is visually distinct from .pf-ui-only",
      "#fff0f0" in css_src and "#f5f5f5" in css_src)


# ─────────────────────────────────────────────────────────────────────────────
# 16. UI marking -- Execution Parameters fields correctly classified
# ─────────────────────────────────────────────────────────────────────────────
section("16. portfolio.js -- Execution tab field marking")

# Live-only fields should have pf-live-only class
check("Product field marked pf-live-only",
      'id="pf-m-product"' in js_src
      and re.search(r"pf-live-only.{0,200}id=\"pf-m-product\"", js_src, re.DOTALL))
check("Strategy Tag marked pf-live-only",
      'id="pf-m-strattag"' in js_src
      and re.search(r"pf-live-only.{0,200}id=\"pf-m-strattag\"", js_src, re.DOTALL))
check("Execution Mode marked pf-live-only",
      'id="pf-m-execmode"' in js_src
      and re.search(r"pf-live-only.{0,200}id=\"pf-m-execmode\"", js_src, re.DOTALL))

# Wired fields should NOT have pf-ui-only class on their row
def row_has_class(html: str, input_id: str, klass: str) -> bool:
    """Look at the .pf-field-row immediately wrapping the given input id."""
    m = re.search(
        r'<div class="(pf-field-row[^"]*)"[^>]*>\s*<span[^>]*>[^<]+</span>\s*<input[^>]*id="' + re.escape(input_id) + '"',
        html, re.DOTALL)
    if not m:
        return False
    return klass in m.group(1)

check("Start Time field row no longer has pf-ui-only",
      not row_has_class(js_src, "pf-m-starttime", "pf-ui-only"))
check("End Time field row no longer has pf-ui-only",
      not row_has_class(js_src, "pf-m-endtime", "pf-ui-only"))


# ─────────────────────────────────────────────────────────────────────────────
# 16b. _is_intraday_bar_type helper (entry window only applies to intraday)
# ─────────────────────────────────────────────────────────────────────────────
section("16b. _is_intraday_bar_type() -- guards entry window from daily-bar misuse")

from core.backtest_runner import _is_intraday_bar_type  # noqa: E402

check("Daily bar -> NOT intraday",
      _is_intraday_bar_type("BTCUSD.BINANCE-1-DAY-LAST-EXTERNAL") is False)
check("Weekly bar -> NOT intraday",
      _is_intraday_bar_type("BTCUSD.BINANCE-1-WEEK-LAST-EXTERNAL") is False)
check("Monthly bar -> NOT intraday",
      _is_intraday_bar_type("BTCUSD.BINANCE-1-MONTH-LAST-EXTERNAL") is False)
check("Minute bar -> intraday",
      _is_intraday_bar_type("EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL") is True)
check("Hour bar -> intraday",
      _is_intraday_bar_type("EURUSD.FOREX_MS-4-HOUR-MID-EXTERNAL") is True)
check("Second bar -> intraday",
      _is_intraday_bar_type("EURUSD.FOREX_MS-30-SECOND-MID-EXTERNAL") is True)
check("Empty / None -> NOT intraday (safe default)",
      _is_intraday_bar_type("") is False
      and _is_intraday_bar_type(None) is False)

# Confirm runner source has the guard wired in both engine sites
check("_run_single_slot wraps entry window filter in _is_intraday_bar_type guard",
      "_is_intraday_bar_type(slot.bar_type_str)" in src)
check("_run_slot_group wraps entry window filter in _is_intraday_bar_type guard",
      "_is_intraday_bar_type(primary_bar_type_str)" in src)
check("entry_window_skipped reason string surfaced in result dict",
      'r["entry_window_skipped"]' in src
      and 'results["entry_window_skipped"]' in src)


# ─────────────────────────────────────────────────────────────────────────────
# 17. End-to-end data flow simulation -- entry window
# ─────────────────────────────────────────────────────────────────────────────
section("17. End-to-end: entry-window flow from UI dict -> filter")

ui_portfolio_with_window = {
    "name": "Window Smoke",
    "description": "",
    "starting_capital": 100_000.0,
    "allocation_mode": "equal",
    "entry_start_time": "09:30",
    "entry_end_time": "16:00",
    "slots": [],
    "created_at": "2024-01-01T00:00:00+00:00",
    "updated_at": "2024-01-01T00:00:00+00:00",
}

p = portfolio_from_dict(ui_portfolio_with_window)
check("Portfolio loads entry window from dict",
      p.entry_start_time == "09:30" and p.entry_end_time == "16:00")

# Apply filter as a worker would
kept, dropped = _filter_bars_by_time_of_day(day_bars, p.entry_start_time, p.entry_end_time)
check("Entry window 09:30..16:00 keeps 391 bars of a 1440-bar day",
      len(kept) == 391 and dropped == 1049)


# ─────────────────────────────────────────────────────────────────────────────
# 18. Dashboard fix: flat_trades + decisive_win_rate
# ─────────────────────────────────────────────────────────────────────────────
section("18. Dashboard fix -- flat_trades + decisive_win_rate")

# 18a. Code presence in backtest_runner.py
check("flat_trades computed in _extract_results",
      "flat_trades = max(total_trades - wins - losses, 0)" in src)
check("decisive_win_rate computed in _extract_results",
      "decisive_win_rate = (wins / _decisive_n * 100) if _decisive_n > 0 else None" in src)
check("flat_trades surfaced in _extract_results return dict",
      '"flat_trades": flat_trades,' in src
      and '"decisive_win_rate": decisive_win_rate,' in src)
check("flat_trades + decisive_win_rate in _extract_slot_from_group_reports",
      "flat_trades = max(trades - wins - losses, 0)" in src
      and "(wins / _decisive_n * 100) if _decisive_n > 0 else None" in src)
check("_merge_portfolio_results aggregates total_flat across slots",
      'total_flat += r.get("flat_trades"' in src)
check("_merge_portfolio_results emits flat_trades + decisive_win_rate",
      '"flat_trades": total_flat,' in src
      and '"decisive_win_rate": decisive_win_rate,' in src)
check("per_strategy entry now carries flat_trades + decisive_win_rate",
      '"flat_trades": r.get("flat_trades"' in src
      and '"decisive_win_rate": r.get("decisive_win_rate"),' in src)

# 18b. UI surfacing in portfolio.js
check("Dashboard renders 'Flat Trades' card",
      'Flat Trades' in js_src)
check("Dashboard renders 'Decisive Win Rate' card",
      'Decisive Win Rate' in js_src)
check("Per-strategy table includes Flat column",
      'P&L rounded to zero' in js_src or '<th title="P&L rounded to zero">Flat</th>' in js_src)
check("Decisive win rate column in per-strategy table",
      '<th title="Wins / (Wins + Losses)">Decisive Win Rate</th>' in js_src)
check("UI falls back gracefully when backend doesn't supply flat_trades",
      'sr.flat_trades != null ? sr.flat_trades : Math.max' in js_src
      and 'r.flat_trades != null' in js_src)

# 18c. Math correctness on the user's actual reproduced case
total_trades = 12052
wins = 339
losses = 180
flat = max(total_trades - wins - losses, 0)
win_rate = wins / total_trades * 100
decisive = (wins / (wins + losses) * 100) if (wins + losses) > 0 else None

check("Math: flat_trades for the user's portfolio = 11533",
      flat == 11533, f"got {flat}")
check("Math: raw win_rate = ~2.81% (unchanged from before fix)",
      abs(win_rate - 2.81) < 0.02, f"got {win_rate:.2f}")
check("Math: decisive_win_rate = ~65.32% (the meaningful number)",
      abs(decisive - 65.32) < 0.02, f"got {decisive:.2f}")
check("Math: wins + losses + flat = total_trades (the invariant)",
      wins + losses + flat == total_trades)

# 18d. HTML report template wiring
report_src = Path("docs/report_template.html").read_text(encoding="utf-8")
check("report_template.html computes tradeFlat",
      "const tradeFlat = totalTrades - tradeWins - tradeLosses" in report_src)
check("report_template.html computes decisiveWinRate",
      "decisiveWinRate = decisiveN ? (tradeWins / decisiveN * 100) : null" in report_src)
check("report_template.html renders Flat Trades KPI",
      'kpi-label">Flat Trades<' in report_src)
check("report_template.html renders Decisive Win% KPI",
      'kpi-label">Decisive Win%<' in report_src)


# ─────────────────────────────────────────────────────────────────────────────
# 19. Path B (BacktestNode) implementation
# ─────────────────────────────────────────────────────────────────────────────
section("19. Path B -- BacktestNode opt-in")

import os as _os
import core.backtest_runner as _br

# 19a. Imports + classes resolve
check("BacktestNode imported into backtest_runner",
      hasattr(_br, "BacktestNode"))
check("BacktestVenueConfig imported",
      hasattr(_br, "BacktestVenueConfig"))
check("BacktestDataConfig imported",
      hasattr(_br, "BacktestDataConfig"))
check("BacktestRunConfig imported",
      hasattr(_br, "BacktestRunConfig"))

# 19b. Helper functions exist
check("_path_b_active helper exists",
      callable(getattr(_br, "_path_b_active", None)))
check("_build_run_config helper exists",
      callable(getattr(_br, "_build_run_config", None)))
check("_path_b_supports_filters helper exists",
      callable(getattr(_br, "_path_b_supports_filters", None)))

# 19c. _path_b_active reads the env flag correctly
_orig = _os.environ.pop("_USE_BACKTEST_NODE", None)
try:
    check("_path_b_active() returns False when flag unset",
          _br._path_b_active() is False)
    _os.environ["_USE_BACKTEST_NODE"] = "1"
    check("_path_b_active() returns True when flag = '1'",
          _br._path_b_active() is True)
    _os.environ["_USE_BACKTEST_NODE"] = "0"
    check("_path_b_active() returns False when flag = '0'",
          _br._path_b_active() is False)
finally:
    _os.environ.pop("_USE_BACKTEST_NODE", None)
    if _orig is not None:
        _os.environ["_USE_BACKTEST_NODE"] = _orig

# 19d. _path_b_supports_filters logic
check("supports_filters(None, None, None) -> True",
      _br._path_b_supports_filters(None, None, None) is True)
check("supports_filters(['Mon'], None, None) -> False",
      _br._path_b_supports_filters(["Mon"], None, None) is False)
check("supports_filters(None, '09:30', None) -> False",
      _br._path_b_supports_filters(None, "09:30", None) is False)
check("supports_filters(None, None, '16:00') -> False",
      _br._path_b_supports_filters(None, None, "16:00") is False)
check("supports_filters([], None, None) -> False (empty list = explicit filter)",
      _br._path_b_supports_filters([], None, None) is False)

# 19e. _build_run_config produces a valid config
rc = _br._build_run_config(
    catalog_path="./catalog",
    instrument_id="EURUSD.FOREX_MS",
    bar_type_strs=[
        "EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL",
        "EURUSD.FOREX_MS-1-MINUTE-ASK-EXTERNAL",
        "EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL",
    ],
    venue="FOREX_MS",
    starting_capital=100_000.0,
    start_date="2024-01-01",
    end_date="2024-12-31",
)
check("_build_run_config returns a BacktestRunConfig",
      isinstance(rc, _br.BacktestRunConfig))
check("RunConfig has 1 venue + 1 data + engine config",
      len(rc.venues) == 1 and len(rc.data) == 1 and rc.engine is not None)
check("Venue starting_balances is list[str] (not Money)",
      rc.venues[0].starting_balances == ["100000.0 USD"])
check("Venue oms_type defaults to NETTING",
      str(rc.venues[0].oms_type).endswith("NETTING") or rc.venues[0].oms_type == "NETTING")
check("Data config carries all 3 bar types (primary + paired)",
      len(rc.data[0].bar_types) == 3)
check("Data config carries date range as ISO strings",
      rc.data[0].start_time == "2024-01-01" and rc.data[0].end_time == "2024-12-31")
check("Engine config has bypass_logging + risk_engine bypass",
      rc.engine.risk_engine is not None and rc.engine.risk_engine.bypass)
check("RunConfig auto-generates an id",
      rc.id is not None and len(rc.id) > 0)
check("chunk_size defaults to None (parity with Path A, no streaming)",
      rc.chunk_size is None)

# Test HEDGING override for slot groups
rc_hedging = _br._build_run_config(
    catalog_path="./catalog",
    instrument_id="EURUSD.FOREX_MS",
    bar_type_strs=["EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL"],
    venue="FOREX_MS",
    starting_capital=300_000.0,
    start_date=None,
    end_date=None,
    oms_type="HEDGING",
)
check("oms_type='HEDGING' override propagates",
      str(rc_hedging.venues[0].oms_type).endswith("HEDGING") or rc_hedging.venues[0].oms_type == "HEDGING")

# 19f. Path B entry points exist and have matching signatures
check("run_backtest_node exists",
      callable(getattr(_br, "run_backtest_node", None)))
check("_run_single_slot_node exists",
      callable(getattr(_br, "_run_single_slot_node", None)))
check("_run_slot_group_node exists",
      callable(getattr(_br, "_run_slot_group_node", None)))

import inspect as _ins
check("run_backtest signature matches run_backtest_node (drop-in)",
      list(_ins.signature(_br.run_backtest).parameters.keys())
      == list(_ins.signature(_br.run_backtest_node).parameters.keys()))
check("_run_single_slot signature matches _run_single_slot_node",
      list(_ins.signature(_br._run_single_slot).parameters.keys())
      == list(_ins.signature(_br._run_single_slot_node).parameters.keys()))
check("_run_slot_group signature matches _run_slot_group_node",
      list(_ins.signature(_br._run_slot_group).parameters.keys())
      == list(_ins.signature(_br._run_slot_group_node).parameters.keys()))

# 19g. Gates wired in source (env flag + filter check at top of each public/worker)
check("run_backtest body checks _path_b_active",
      "if _path_b_active():" in src
      and "return run_backtest_node(" in src)
check("_run_single_slot body checks _path_b_active + _path_b_supports_filters",
      "if _path_b_active() and _path_b_supports_filters(" in src
      and "return _run_single_slot_node(" in src)
check("_run_slot_group body checks _path_b_active + _path_b_supports_filters",
      "if _path_b_active() and _path_b_supports_filters(" in src
      and "return _run_slot_group_node(" in src)
check("Path B variants tag their results with 'path_b': True",
      'r["path_b"] = True' in src
      and 'results["path_b"] = True' in src)
check("per_strategy entries propagate path_b flag",
      '"path_b": bool(r.get("path_b"))' in src)
check("_merge_portfolio_results emits portfolio-level path_b summary (all/mixed/none)",
      'path_b_summary = "all"' in src
      and 'path_b_summary = "mixed"' in src
      and '"path_b": path_b_summary' in src)
check("Dashboard renders a Path A / Path B badge on results",
      'r.path_b === "all"' in js_src
      and 'Path B' in js_src
      and 'pathBadge' in js_src)


# ─────────────────────────────────────────────────────────────────────────────
# Final summary
# ─────────────────────────────────────────────────────────────────────────────
print()
if failures:
    print(f"{RED}=== {len(failures)} FAILURE(S) ==={RESET}")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
else:
    print(f"{GREEN}=== ALL CHECKS PASSED ==={RESET}")
    sys.exit(0)
