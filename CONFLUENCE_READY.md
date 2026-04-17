# Rogue Bot — Technical Documentation

**Version:** 1.2  
**Release date:** 2026-04-17  
**Instrument:** XAU/USD M15  
**Base:** Rogue v1.0

---

## Overview

Rogue is an automated gold trading bot. It trades XAU/USD on the M15 timeframe using CPR (Central Pivot Range) breakout signals. The bot includes a full suite of entry guards to prevent low-quality entries, protect from stale re-entries, and manage session-based risk.

---

## Architecture

### Core cycle (every 5 min)

```
1. Session check            is this a valid trading window?
2. News filter              any high-impact events within +/-30 min?
3. Signal engine            does a valid CPR breakout setup exist?
4. H1 trend filter          does H1 EMA21 align with direction?
5. Scoring gate             is score >= 4/6?
6. Same Setup Guard         was the same setup closed within 30 min?
7. Direction cooldown       SL in this direction within 120 min?
8. Consecutive SL guard     1+ SL in same direction? Hard block.
9. Daily/session loss caps  3 losses/day or 2 losses/session hit?
10. Position sizing         calculate units from SL distance
11. RR gate                 is actual RR >= minimum?
12. Place order             OANDA market order with SL/TP
13. Persist trade record    SQLite + Telegram alert
```

---

## Signal Engine

### CPR Levels
Fetched daily from OANDA M15 candles.

| Level | Description |
|---|---|
| Pivot | Central reference point |
| TC / BC | Top/bottom of CPR range |
| R1 / R2 | Resistance levels |
| S1 / S2 | Support levels |
| PDH / PDL | Previous day high/low |

### Setup Types
| Setup | Condition |
|---|---|
| R2 Extended Breakout | Price > R2, BUY direction |
| S2 Extended Breakdown | Price < S2, SELL direction |
| CPR Bear Breakdown | Price below CPR range, SELL |
| CPR Bull Breakout | Price above CPR range, BUY |

### Scoring (minimum 4/6)
| Check | Points |
|---|---|
| Extended entry (main condition) | +1 |
| H1 trend aligns | +1 |
| Both SMAs align with direction | +2 |
| Narrow CPR (< 0.5%) | +2 |
| Exhaustion penalty (>1.5x ATR) | -1 |

Score 4 = $66 partial. Score 5–6 = $100 full.

---

## Entry Guards

### Same Setup Guard
Prevents re-entering the same setup immediately after a close.

**Stale re-entry conditions (ALL must match):**
1. Same setup name
2. Same direction (BUY / SELL)
3. Same CPR pivot (within 1.0 pip)
4. Within same_setup_guard_candles x 15 min of last CLOSE

**Key detail:** Uses closed_at_sgt (close time), not timestamp_sgt (open time).

**Log:**
```
[SAME_SETUP_GUARD] Same Setup Guard blocked: 'R2 Extended Breakout' BUY
— same CPR levels (pivot=4652.6), closed 2min ago (min=30min / 2 candles)
```

### Direction Cooldown (Rogue v1.0 fix)
After ANY SL hit, blocks the same direction for 120 min immediately.

**How it works:**
- When backfill_pnl detects a closed SL trade, immediately writes
  direction_block_DIRECTION to runtime_state.json with timestamp+120min
- Every subsequent cycle checks this timestamp before allowing entry
- Fires on the FIRST SL — no waiting for the 2nd

**Why this matters (real example Apr 13):**
```
T7  SELL SL at 08:59  →  direction_block_sell set until 10:59
T8  SELL tries 09:30  →  [DIRECTION_BLOCK] 89min remaining — BLOCKED
```
Previous behaviour (v5.3): cooldown only fired after 2nd SL. T8 was allowed and lost.

**Log:**
```
[DIRECTION_BLOCK] SL detected on trade 622 — SELL blocked until 10:59 SGT (120min cooldown)
[DIRECTION_BLOCK] Direction cooldown active — SELL blocked for 89min more (SL streak=1)
```

**Settings:**
```json
"consecutive_sl_guard": 1,
"sl_direction_cooldown_min": 120
```

### Exhaustion Block
Blocks S2/R2 extended setups when price stretch exceeds 1.5x ATR from SMA20.

**Tightened from 2.5x in v1.0:**
In normal markets: ATR ~13 pips → 1.5x = 19.5 pip stretch max
In volatile markets (post-tariff spike): ATR ~23 pips → still 34.5 pip max

The 2.5x threshold in v5.3 allowed 57.5 pip stretch entries in volatile conditions —
too wide, resulting in chasing exhausted moves.

**Settings:**
```json
"exhaustion_atr_mult": 1.5
```

### H1 Trend Filter (HARD)
- BUY blocked if H1 price < EMA21 (bearish)
- SELL blocked if H1 price > EMA21 (bullish)
- No soft mode — always a hard block

### News Filter
- Blocks entries +/-30 min around high-impact USD events
- Cache refreshed every 60 min from Forex Factory

---

## Risk Controls

| Control | Value | Change from v5.3 |
|---|---|---|
| Max losses / day | 3 | Was 8 — caused 5-trade losing streaks |
| Max losses / session | 2 | Was 4 |
| Direction cooldown | 120 min | Was 60 min — fired too late |
| Consecutive SL guard | 1 | Was 2 — 2nd SL already happened by firing |
| Exhaustion threshold | 1.5x | Was 2.5x — too wide in volatile markets |

---

## Sessions (SGT)

| Session | Window | Min Score | Cap |
|---|---|---|---|
| Dead zone | 00:00–07:59 | — | No entries |
| Asian | 08:00–15:59 | 4 | 5 |
| London | 16:00–20:59 | 4 | 10 |
| US | 21:00–00:59 | 4 | 10 |

---

## Trade Lifecycle

```
Signal detected → Order placed → FILLED
    ↓
Every 5-min cycle: backfill_pnl checks if trade closed
    ↓
Trade closed (TP or SL)?
    ↓ SL                           ↓ TP
direction_block set +120min     No block set
closed_at_sgt stored            closed_at_sgt stored
Same Setup Guard armed          Same Setup Guard armed
Telegram: trade closed          Telegram: trade closed
```

---

## Telegram Alerts

### Startup
```
Rogue v1.2 started
Mode: DEMO | Balance: $5,000.00
Pair: XAU/USD (M15)
```

### Direction block set (on SL close)
```
[DIRECTION_BLOCK] SL detected on trade 622
SELL blocked until 10:59 SGT (120min cooldown)
```

### Direction block enforced (on next cycle)
```
BLOCKED: Direction cooldown active
SELL blocked for 89min more (SL streak=1, cooldown=120min)
```

### Same Setup Guard
```
BLOCKED: Same Setup Guard
R2 Extended Breakout BUY — closed 2min ago
```

---

## Bug History

### Same Setup Guard — open-time bug (fixed in v5.3, inherited in v1.0)
Guard used trade open time instead of close time. Long-duration trades
that closed and re-triggered within 30 min were missed.
Fix: uses closed_at_sgt. Cost before fix: -$105.72 (T590, Apr 8).

### Direction cooldown — fires-too-late bug (fixed in Rogue v1.0)
Cooldown only fired after 2nd consecutive SL in same direction.
The 2nd SL already happened before the block was set.
Fix: direction_block set immediately in backfill_pnl on any SL.
Cost before fix: T7→T8 sequence on Apr 13 (-$96.25 avoidable loss).

---

## Key Settings Reference

```json
{
  "bot_name": "Rogue v1.2",
  "version": "1.2",
  "rr_ratio": 1.5,
  "sl_min_usd": 25.0,
  "sl_max_usd": 60.0,
  "sl_mode": "atr_based",
  "signal_threshold": 4,
  "position_full_usd": 100,
  "position_partial_usd": 66,
  "h1_trend_filter_enabled": true,
  "h1_ema_period": 21,
  "require_candle_close": true,
  "same_setup_guard_enabled": true,
  "same_setup_guard_candles": 2,
  "consecutive_sl_guard": 1,
  "sl_direction_cooldown_min": 120,
  "exhaustion_atr_mult": 1.5,
  "news_filter_enabled": true,
  "news_block_before_min": 30,
  "news_block_after_min": 30,
  "max_losing_trades_day": 3,
  "max_losing_trades_session": 2,
  "max_trades_day": 20,
  "trailing_stop_atr_mult": 0,
  "breakeven_enabled": false
}
```

---

## Version History

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-04-16 | Initial Rogue release. 7 fixes applied from prior research: direction cooldown fires on first SL, tightened exhaustion 2.5x→1.5x, hard daily/session loss caps 8→3/4→2, cooldown 60→120min, guard fires on streak=1, Same Setup Guard close-time fix, full rename. |
| 1.1 | 2026-04-17 | Wired daily Telegram report time to settings (`daily_report_hour_sgt` / new `daily_report_minute_sgt`); was hardcoded 15:30 SGT in v1.0. Fixed stale 09:30 SGT docstrings in `reporting.py`. Default daily report time now 08:00 SGT. |
| 1.2 | 2026-04-17 | Fixed two display-only bugs in the startup Telegram message: (1) `Global cap` now reads `max_concurrent_trades` from settings (was falling back to default `2` even when setting was `1`); (2) Asian session threshold now reads the `Asian` key in `session_thresholds` (was looking up legacy `Tokyo` key → defaulting to `min_score + 1`). Bot enforcement was always correct; only the startup Telegram display was wrong. |
