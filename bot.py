import os
import requests

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

def send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": chat_id,
        "text": text
    })

def main():
    url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    response = requests.get(url).json()

    if not response.get("ok"):
        return

    for update in response.get("result", []):
        message = update.get("message", {})
        chat = message.get("chat", {})
        text = message.get("text", "")

        if not chat:
            continue

        chat_id = chat["id"]

        if text == "/start":
            send_message(
                chat_id,
                "🤖 SMC Crypto Bot\n\n"
                "بەخێربێیت!\n"
                "بۆتەکە بە سەرکەوتوویی کار دەکات. ✅\n\n"
                "/status — بارودۆخی بۆت"
            )

        elif text == "/status":
            send_message(
                chat_id,
                "🟢 Bot Status: ONLINE\n"
                "📊 Market Analysis: Coming soon\n"
                "🧠 SMC Engine: Coming soon"
            )

if __name__ == "__main__":
    main()
