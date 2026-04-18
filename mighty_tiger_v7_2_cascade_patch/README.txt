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
