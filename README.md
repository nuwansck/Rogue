# Rogue — Automated XAU/USD Trading Bot

**Version:** 1.0  
**Instrument:** XAU/USD (Gold) — M15  
**Platform:** OANDA REST API v20  
**Stack:** Python 3.13 · APScheduler · SQLite · Telegram  
**Deploy:** Railway (Singapore region)

---

## Overview

Rogue is an automated trading bot for gold (XAU/USD) on the M15 timeframe. It uses CPR (Central Pivot Range) breakout signals with a layered entry guard system to filter quality entries and protect capital.

---

## Strategy

### Signal Engine
- CPR pivot breakout — S2/R2 extended levels
- H1 EMA21 trend filter (HARD) — blocks counter-trend entries
- Scoring system 4–6/6 — minimum 4 required to enter
- Candle close confirmation — no premature entries on wicks

### Position Sizing
| Score | Size |
|---|---|
| 5–6 | $100 (full) |
| 4 | $66 (partial) |
| < 4 | No trade |

### Risk per Trade
| Parameter | Value |
|---|---|
| SL | ATR-based, $25–$60 |
| TP | RR 1.5 (~37.5 pips) |
| Trailing stop | Off |
| Breakeven | Off |

---

## Entry Guards

| Guard | Description |
|---|---|
| **Same Setup Guard** | Blocks re-entry if same setup closed within 30 min. Uses close time not open time. |
| **Direction block** | 120 min hard cooldown after ANY SL — fires on first loss, no score bypass |
| **Consecutive SL guard** | Hard blocks after 1 SL in same direction |
| **News filter** | +/-30 min around high-impact USD events |
| **Exhaustion block** | Blocks S2/R2 setups at >1.5x ATR stretch (tightened from 2.5x) |

---

## Sessions (SGT)

| Session | Window | Cap | Min Score |
|---|---|---|---|
| Asian | 08:00–15:59 | 5 | 4 |
| London | 16:00–20:59 | 10 | 4 |
| US | 21:00–00:59 | 10 | 4 |
| Dead zone | 00:00–07:59 | — | No entries |

---

## Risk Controls

| Control | Value | Reason |
|---|---|---|
| Max losses / day | 3 | Hard stop — prevents 5-loss streaks |
| Max losses / session | 2 | Per-session hard stop |
| Direction cooldown | 120 min | After any SL — was 60 min, fired too late |
| Loss streak cooldown | 30 min | After 2 losses any direction |
| Max trades / day | 20 | Overall cap |
| Friday cutoff | 23:00 SGT | Weekend risk |
| Day reset | 08:00 SGT | Daily counters |

---

## Deployment

### Environment Variables (Railway)
```
OANDA_API_KEY       = <demo API key>
OANDA_ACCOUNT_ID    = <demo account ID>
TELEGRAM_BOT_TOKEN  = <telegram bot token>
TELEGRAM_CHAT_ID    = <telegram chat ID>
```

### Persistent Volume
Mount at /data — stores: settings.json, trade_history.json,
calendar_cache.json, runtime_state.json, rogue.db

### Health Check
GET /health on port 8080

### Startup Confirmation
```
Rogue v1.0 started
Mode: DEMO | Balance: $X,XXX.XX
Pair: XAU/USD (M15)
```

---

## Log Signatures

| Log | Meaning |
|---|---|
| === Rogue v1.0 | 16:00 SGT === | Cycle start |
| CPR signal | setup=R2 Extended Breakout | dir=BUY | score=5/6 | Signal detected |
| Trade placed! ID: 584 | Fill price: 4799.525 | Entry confirmed |
| [SAME_SETUP_GUARD] blocked: ... | Stale re-entry blocked |
| [DIRECTION_BLOCK] SL detected — BUY blocked until 18:30 SGT | Cooldown set |
| [DIRECTION_BLOCK] Direction cooldown active — ... | Cooldown enforced |
| CPR signal BLOCKED (extended+exhaustion) | Exhaustion filter fired |

---

## Key Settings

```json
{
  "bot_name": "Rogue v1.0",
  "version": "1.0",
  "rr_ratio": 1.5,
  "sl_min_usd": 25.0,
  "sl_max_usd": 60.0,
  "sl_mode": "atr_based",
  "signal_threshold": 4,
  "position_full_usd": 100,
  "position_partial_usd": 66,
  "h1_trend_filter_enabled": true,
  "h1_ema_period": 21,
  "same_setup_guard_enabled": true,
  "same_setup_guard_candles": 2,
  "consecutive_sl_guard": 1,
  "sl_direction_cooldown_min": 120,
  "exhaustion_atr_mult": 1.5,
  "max_losing_trades_day": 3,
  "max_losing_trades_session": 2,
  "news_filter_enabled": true,
  "trailing_stop_atr_mult": 0,
  "breakeven_enabled": false
}
```

---

## Version History

| Version | Date | Change |
|---|---|---|
| 1.0 | 2026-04-16 | Initial release. CPR breakout + H1 filter + Same Setup Guard (close-time fix) + direction cooldown fires on first SL + tightened exhaustion + hard daily/session loss caps. |
| 1.1 | 2026-04-17 | Wired daily Telegram report time to settings (`daily_report_hour_sgt` / new `daily_report_minute_sgt`); was hardcoded 15:30 SGT in v1.0. Fixed stale 09:30 SGT docstrings in `reporting.py`. Default daily report time now 08:00 SGT (aligned with trading-day boundary). |
| 1.2 | 2026-04-17 | Fixed two display bugs in the `msg_startup` Telegram message: (1) `Global cap` now reads `max_concurrent_trades` from settings instead of defaulting to 2; (2) Asian session threshold now reads from the `Asian` key in `session_thresholds` (was looking up legacy `Tokyo` key → falling back to `min_score + 1`). Display-only — bot enforcement was always correct. |
| 1.3 | 2026-04-17 | "Safe and workable" release aligning closer to base CPR spec. 4 changes: (1) `signal_threshold` raised 4→5 — blocks weakest setups where a single strong component carries the score; (2) wide CPR (>1.0% of pivot) now HARD-blocked — previously just gave 0 score; new setting `cpr_width_block_pct` controls threshold; (3) breakeven re-enabled (`breakeven_enabled: true`) — when unrealized ≥ 1× SL risk, partial-close 50% + move SL to BE; (4) breakeven SL now offset by entry spread (new `breakeven_spread_adjust: true`) so BE stop-outs truly net zero PnL instead of small negative = spread. |
