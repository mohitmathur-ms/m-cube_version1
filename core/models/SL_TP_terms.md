# Stop-Loss & Take-Profit Terms — Plain-Language Guide

A glossary of every Stop-Loss (SL) and Take-Profit (TP / "Target") setting in the
portfolio data model, in simple language, with a worked example for each.

- **Schema (the fields):** [`core/models/exit_config/__init__.py`](exit_config/__init__.py) → `ExitConfig`
- **Engine (the behaviour):** [`core/managed_strategy.py`](../managed_strategy.py) → `ManagedExitStrategy`

> **One rule to remember the directions.** A **stop-loss protects against an adverse
> move**, so it sits on the *losing* side of your entry: **below** entry for a long
> (BUY), **above** entry for a short (SELL). A **target/take-profit** sits on the
> *winning* side: **above** entry for a long, **below** entry for a short.

The engine's single price helper is `_compute_sl_price(is_long, pct)`
([managed_strategy.py:2647](../managed_strategy.py#L2647)):

```
long  : price = entry × (1 − pct/100)
short : price = entry × (1 + pct/100)      (then snapped to the instrument tick)
```

A **positive** `pct` puts the price on the *loss* side (used for a normal SL); a
**negative** `pct` puts it on the *profit* side (used by trailing to lock gains).

---

## 1. `stop_loss_type` — *how* the stop is sized

The method used to place the stop. One of:

| Value | Meaning | Initial stop price |
|---|---|---|
| `"none"` | No stop loss at all. | — |
| `"percentage"` | Stop a % of entry away. | long `entry×(1−v/100)`, short `entry×(1+v/100)` |
| `"points"` | Stop a fixed number of price points away. | long `entry−v`, short `entry+v` |
| `"trailing"` | Stop starts at `stop_loss_value` distance, then **ratchets into profit** as price moves your way (see §4–§5). | long `entry×(1−v/100)`, short `entry×(1+v/100)` — with `v` usually `0` |
| `"atr"` | Stop sized from market volatility (Average True Range) at entry. | long `entry − mult×ATR`, short `entry + mult×ATR` |

**Example.** Long entry at 100, `stop_loss_type="percentage"`, `stop_loss_value=1`
→ stop at `100×(1−1/100) = 99`. If price falls to 99, you're stopped out.

---

## 2. `stop_loss_value` — *how far* the stop sits

The number that goes with `stop_loss_type`:

- with `"percentage"` → a **percent** (e.g. `0.03` means 0.03%).
- with `"points"` → an **absolute distance** in price points (e.g. `8` = 8 points).
- with `"trailing"` → the **initial** stop distance *before* trailing begins. **It is
  often `0`**, which means the stop starts exactly at the entry price (break-even).
- with `"atr"` / `"none"` → ignored.

**Example (the gotcha).** A leg labelled `_SL_trailing_0.03` in the test portfolio has
`stop_loss_type="trailing"`, `stop_loss_value=0`. So its **initial** stop is
`entry×(1+0/100) = entry` for a short. The `0.03` in the label is *not* the stop
distance — it is the trailing **offset** (see §5). A short entered at 24858.10 gets its
stop at **24858.10** (break-even), so a single tick up to 24858.95 stops it out
immediately. That is correct behaviour for `stop_loss_value=0`, not a bug.

---

## 3. `sl_value_is_absolute` — read the value as points or percent

An override flag for how `stop_loss_value` is interpreted, independent of the type name
(spec "Premium" input mode):

- `True` → treat the value as an **absolute** point distance: `sl = entry ∓ value`.
- `False` → treat it as a **percent**: `sl = entry × (1 ∓ value/100)`.
- `None` → **infer** from the type name (`percentage`→%, `points`→absolute). This is the
  normal/default case.

**Example.** `stop_loss_value=8` with `sl_value_is_absolute=True` → an 8-**point** stop.
The same `8` with `sl_value_is_absolute=False` would mean **8%** — a very different stop.
(`target_value_is_absolute` is the identical flag for the target side.)

---

## 4. `trailing` (stop_loss_type = "trailing") — the moving, profit-locking stop

A trailing stop does **not** move against you. It starts at the initial distance
(`stop_loss_value`, often `0` = break-even) and then, as the trade gains, it **ratchets
in the profit direction only** and never loosens. It locks in gains step-by-step using
the two knobs below.

Engine logic ([managed_strategy.py:1925-1937](../managed_strategy.py#L1925-L1937)):

```python
steps = int(highest_profit / trailing_sl_step)        # how many full steps of profit so far
if steps > 0:
    trail_offset = steps * trailing_sl_offset
    trail_sl = _compute_sl_price(is_long, -trail_offset)   # negative pct → profit side
    # tighten-only: move the stop only if the new level is better
```

`highest_profit` is the best profit-% the trade has reached so far
([managed_strategy.py:1753](../managed_strategy.py#L1753)).

---

## 5. `trailing_sl_step` and `trailing_sl_offset` — the two trailing knobs

These only matter when `stop_loss_type="trailing"` (and on the post-Move-SL ratchet).

- **`trailing_sl_step`** — the **trigger increment**. For every `step` % of profit gained,
  the stop advances one notch. Think *"every time I make another X% profit…"*.
- **`trailing_sl_offset`** — the **lock amount per step**. Each notch moves the stop
  `offset` % further onto the profit side of entry. Think *"…lock in another Y%."*

So after `n` completed steps the stop sits at `n × offset` % **into profit** from entry.

**Worked example.** Long, entry = 100, `trailing_sl_step=0.05`, `trailing_sl_offset=0.03`,
`stop_loss_value=0`:

| Best profit reached | Completed steps `int(profit/0.05)` | Locked offset `steps×0.03` | Stop price `entry×(1+offset/100)` |
|---|---|---|---|
| 0.00% (just entered) | 0 | 0.00% | 100.000 (break-even) |
| 0.04% | 0 | 0.00% | 100.000 (not enough for 1 step yet) |
| 0.05% | 1 | 0.03% | 100.030 (now locking a small profit) |
| 0.12% | 2 | 0.06% | 100.060 |
| 0.20% | 4 | 0.12% | 100.120 |

The stop only ever climbs (for a long) — if profit later falls back, the stop stays put
and eventually the price hits it, exiting with the locked-in gain. For a **short** it is
the mirror: the stop only ever descends, locking profit as price falls.

---

## 6. `sl_atr_period` and `sl_atr_multiplier` — volatility-based stop

Used when `stop_loss_type="atr"`. Instead of a fixed distance, the stop is sized from the
market's recent volatility (Average True Range) measured over `sl_atr_period` bars at
entry:

```
long  SL = entry − sl_atr_multiplier × ATR
short SL = entry + sl_atr_multiplier × ATR
```

**Example.** `sl_atr_period=14`, `sl_atr_multiplier=0.5`. If ATR(14)=20 points and you are
long at 100, the stop is `100 − 0.5×20 = 90`. Calmer markets (smaller ATR) give a tighter
stop; choppier markets give a wider one. The target side has the identical
`tgt_atr_period` / `tgt_atr_multiplier` pair.

---

## 7. `target_type` and `target_value` — the take-profit (mirror of SL)

Exactly like the stop, but on the winning side:

| `target_type` | Target price |
|---|---|
| `"none"` | No take-profit. |
| `"percentage"` | long `entry×(1+v/100)`, short `entry×(1−v/100)` |
| `"points"` | long `entry+v`, short `entry−v` |
| `"atr"` | long `entry + mult×ATR`, short `entry − mult×ATR` |

`target_value` is the number (% or points), and `target_value_is_absolute` is the same
points-vs-percent override described in §3.

**Example.** Short entry at 100, `target_type="points"`, `target_value=8` → target at
`100−8 = 92`. If price drops to 92, the position takes profit.

---

## 8. `target_lock_trigger` and `target_lock_minimum` — one-shot stop upgrade

A **one-time** move of the stop the first moment profit gets big enough — simpler than full
trailing. Logic at [managed_strategy.py:1884-1886](../managed_strategy.py#L1884-L1886):

- **`target_lock_trigger`** — the profit-% that, once reached, fires the upgrade.
- **`target_lock_minimum`** — where the stop is then placed, as a %-from-entry distance
  (same convention as a percentage SL).

Once `highest_profit ≥ target_lock_trigger`, the stop jumps once to the
`target_lock_minimum` level and stays there (it does not keep ratcheting like trailing).

**Example.** `target_lock_trigger=0.20`, `target_lock_minimum=0.05`. The trade runs to
+0.20% profit → the stop is upgraded a single time to the 0.05% level, protecting most of
the move. (For continuous step-by-step locking, use trailing in §4–§5 instead.)

---

## 9. Leg-level Trailing **Target** — `tgt_trail_*` (profit-lock that exits on pullback)

A ratcheting profit-lock on the **target** side. Unlike trailing-SL (which tightens a
stop), this watches profit and **exits the leg when profit falls back to a locked floor**.
This is what produces the `Trailing Target: profit +X% fell to locked floor +Y%` exit
reason in the order book.

- **`tgt_trail_enabled`** — turn it on.
- **`tgt_trail_when_profit_reach`** — profit-% at which the lock first activates.
- **`tgt_trail_lock_min_profit`** — the floor locked in when it activates.
- **`tgt_trail_every`** — raise the floor for every this-much additional profit.
- **`tgt_trail_by`** — how much the floor rises per `tgt_trail_every`.

**Example.** `when_profit_reach=0.10`, `lock_min_profit=0.05`, `every=0.05`, `by=0.03`.
Profit reaches +0.10% → floor locks at +0.05%. Profit climbs to +0.15% (one more `every`)
→ floor rises to +0.08%. If profit then slips back down to the floor, the leg exits there,
banking the locked profit.

---

## 10. `sl_wait_sec` / `sl_wait_bars` and `tgt_wait_sec` / `tgt_wait_bars` — confirmation gates

A breach must **persist** before the exit actually fires — this filters out one-tick
spikes. `*_sec` is wall-clock seconds (preferred); `*_bars` is the legacy bar-count gate.
If both are set, the seconds value wins. The target side (`tgt_wait_*`) works the same and
resets if price retreats back inside the target.

**Example.** `sl_wait_sec=5`: price pokes through the stop but, if it comes back within 5
seconds, no exit; only a breach still in force after 5 seconds triggers the stop.

---

## 11. `on_sl_action` / `on_target_action` — what happens *after* a hit

What the engine does once the SL or TP fires:

| Value | Behaviour |
|---|---|
| `"close"` | Square off — flatten and stay flat. |
| `"re_execute"` | Flatten, then immediately re-arm the entry signal (capped by `max_re_executions`). |
| `"reverse"` | Close and open the **opposite** side (this is the `Reverse on SL/TP` you see in the book). |
| `"execute"` | Flatten and arm a sibling slot named in `execute_target_leg_id`. |
| `"re_entry"` | Flatten, then wait for `reentry_price` before re-entering (capped by `max_re_entries`). |
| `"keep_leg_running"` | Ignore the trigger; leave the position open with SL/TP disarmed for this trade. |

Up to three actions can be combined as a comma-separated string (e.g.
`"re_execute,execute"`).

**Example.** A short hits its stop with `on_sl_action="reverse"` → it closes the short and
opens a long, logged as `Reverse on SL`.

---

## 12. Supporting knobs (quick reference)

| Field | Meaning |
|---|---|
| `exit_price_format` | Which price series triggers consult: `"ohlcv"` (bar high/low, default), `"ltp"` (last price/close), `"bidask"` (SELL on bid, BUY on ask). |
| `max_re_executions` | Cap on `re_execute` actions per position. |
| `execute_target_leg_id` | Sibling slot armed by the `execute` action. |
| `reentry_price` / `max_re_entries` | Price the `re_entry` action waits for, and its daily cap (`0` = unlimited). |
| `armed_at_start` | If `False`, the leg ignores its own signals until a sibling's `execute` arms it. |
| `squareoff_time` / `squareoff_tz` | Leg-level forced close at a local time each day (most specific square-off level). |

---

## Putting it together — the test-portfolio trailing leg

Config: `stop_loss_type="trailing"`, `stop_loss_value=0`, `trailing_sl_step=0.05`,
`trailing_sl_offset=0.03`. Short entered at **24858.10**.

1. **Initial stop = entry = 24858.10** (because `stop_loss_value=0` → break-even start).
2. No favourable move yet → `highest_profit≈0` → `steps=0` → stop stays at entry.
3. Next tick rises to 24858.95 → `price ≥ SL` → stop fires (`Stop Loss: price=24858.95 ≥
   SL=24858.10`).

Had the trade first fallen (profit for a short) by 0.05%, `steps` would become 1 and the
stop would drop to `24858.10×(1−0.03/100) ≈ 24850.65`, locking a small gain. Because the
stop began at break-even, this configuration whipsaws out quickly on any immediate adverse
tick — a deliberate consequence of `stop_loss_value=0`, not a defect.
