# SAIWAN 4H Triangle Telegram Bot

This is a fresh version for Bitget USDT perpetuals on the **4H timeframe**.

Signal flow:
**Symmetrical Triangle -> Breakout -> Retest -> Confirmation -> LONG**

The bot uses CLOSED 4H candles for signals. It does not use the currently forming candle.

## Railway

1. Upload/push these files to a new GitHub repository.
2. Create a Railway service from that repository.
3. Add Variables:
   - `TELEGRAM_BOT_TOKEN`
   - `TELEGRAM_CHAT_ID`
4. In Railway Service Settings -> Deploy -> Start Command, use:

`python main.py`

Do NOT use `gunicorn` for this Telegram worker. Railway supports a direct Python start command. A Procfile is included as a fallback.

## Telegram

- `/start`
- `/status`
- `/scan`

## Important

This is a pattern scanner, not a guarantee of profitable trades. The strict filters are designed to reduce false signals, but market data can still produce false breakouts.
