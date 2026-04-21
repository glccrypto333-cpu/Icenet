Start command: python bot_V7_unified_levels.py
Use existing Railway vars BOT_TOKEN and CHAT_ID or env mapping per project.

V7.2 patch notes:
- MONSTER is now the final unified level in the main 15m aggregate scale
- After MONSTER, level no longer grows; only CASCADE grows in +100k steps
- CASCADE is counted from the first MONSTER base value
- CASCADE resets if the next +100k step is not reached within range_reset_sec
- "MONSTER 5ч" text removed from UI
- Level names in messages/UI use HYPER / SUPER HYPER / MONSTER
- Funding in message stats is fetched from Bybit

V7.2.1 patch notes:
- HELP shows current build version and upload date
- MONSTER message always shows CASCADE line, starting from +0$
- Added cascade wait debug logs: base / extra / next step / left to next step


V7.3 patch notes:
- Fixed post-MONSTER reset logic
- After range_reset_sec without a new cascade step, the whole cycle is reset as a normal 1–10 cycle
- On reset, local 1m and aggregate 15m flow windows for the symbol are cleared
- New cycle starts from fresh flow only, instead of inheriting stale MONSTER/SUPER HYPER aggregate state


V7.3_final patch:
- timezone fixed to Europe/Berlin
- timer marker shown on the same bottom line with spacing, not as buttons
- OI block format restored to 3 lines
- no separate buttons below message
- monster reset fix preserved


COPY fix:
- removed plain text COPY placeholder
- now ticker is shown as monospace code in bottom line for quick long-press copy
- replaced HTML &nbsp; spacing with Unicode spacing to avoid visible &nbsp; in Telegram

V7_work patch:
- BUILD_VERSION synced to V7_work
- startup checklist title now uses dynamic BUILD_VERSION instead of old hardcoded V7_unified_levels
- BUILD_DATE updated to 2026-04-19
- logic intentionally unchanged: no state engine, reset, MONSTER or CASCADE behavior changes



V7_work.2:
- BUILD_VERSION обновлён до V7_work.2
- Таймер: ✅ = сценарий жив, 🆕 = новый сценарий, ❌ = anomaly flag lifecycle/reset
- ❌ автоматически логируется в отдельный debug chat при заполненном telegram.debug_chat_id
- В строке ссылок после тикера добавлен дополнительный отступ перед таймером


V7_work.2 patch notes:
- Added daily cycle number per symbol in signal header: (1), (2), ...
- Cycle counter is counted on top of normal reset logic and does not modify core lifecycle
- Added TOP 4h for Binance and Bybit
- Added OFF mode via very high thresholds
- Added separate Reload config and Restart bot actions
- Added Mute list control in Help (runtime only, cleared on restart)
- Added daily automatic ticker reload / bot restart at 03:00 MSK
- Updated metric marks with arrows and new exclamation thresholds
- Updated Help text and control menus
