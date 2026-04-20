"""Общий Telegram-sender для multi-airport collectors."""
import requests


def send_telegram(text, token, chat_id, prefix=None):
    """Шлёт HTML-сообщение в Telegram.

    Args:
        text: тело сообщения (HTML)
        token: TG bot token
        chat_id: target chat ID
        prefix: опциональный префикс (например "[HKT]" / "[CXR]"), добавляется в начало
    """
    if not token or not chat_id:
        return
    if prefix:
        text = f"{prefix} {text}"
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass
