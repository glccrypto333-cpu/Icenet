# Liq Bot Railway package

## Files
- `bot.py` — bot with Railway env support
- `config.yaml` — sanitized config without secrets
- `requirements.txt` — Python deps
- `.gitignore`
- `railway.toml` — start command for Railway

## Railway variables
Set these in Railway → Variables:
- `BOT_TOKEN`
- `CHAT_ID`

## Deploy
1. Push these files to GitHub
2. Railway → New Project → GitHub Repository
3. Select repo
4. Deploy

Logs should show:
- ✅ BOT STARTED / final_v1_3
- ✅ Binance connected
- ✅ Bybit symbols loaded
- ✅ Bybit subscribe sent
