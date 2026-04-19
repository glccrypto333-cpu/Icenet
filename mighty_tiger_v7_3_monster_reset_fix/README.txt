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
