import os
import json
import datetime
import logging
import requests

from dotenv import load_dotenv
from google.cloud import storage

# === Загрузка переменных окружения ===
load_dotenv()
logging.basicConfig(level=logging.INFO)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_FILE_NAME = os.getenv("GCS_FILE_NAME")

# service account json как СТРОКА в .env (GCS_KEY_JSON)
GCS_KEY_JSON = os.getenv("GCS_KEY_JSON")

# === Временные слоты (оставляю как у тебя) ===
TIME_SLOTS = {
    'morning': range(1, 2),      # 05:00–08:59 (комментарий твой)
    'day': range(3, 4),          # 09:00–12:59
    'afternoon': range(7, 8),    # 13:00–16:59
    'evening': range(11, 12)     # 17:00–22:59
}

POSTED_LOG_FILE = "posted_log.json"  # объект в GCS

# --------- общие утилиты ---------
def _ensure_gcs_credentials():
    """Пишем ключ из env в файл и назначаем GOOGLE_APPLICATION_CREDENTIALS."""
    if not GCS_KEY_JSON:
        raise ValueError("❌ GCS_KEY_JSON не установлена или пуста")
    key_dict = json.loads(GCS_KEY_JSON)
    with open("gcs_key.json", "w", encoding="utf-8") as f:
        json.dump(key_dict, f)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcs_key.json"

def _gcs_client():
    return storage.Client()

# --------- доменные функции ---------
def get_current_slot():
    hour = datetime.datetime.now().hour
    for slot, hours in TIME_SLOTS.items():
        if hour in hours:
            return slot
    return None

def download_json_from_gcs():
    _ensure_gcs_credentials()
    client = _gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_FILE_NAME)
    content = blob.download_as_string()
    return json.loads(content)

def load_posted_log_from_gcs():
    try:
        _ensure_gcs_credentials()
        client = _gcs_client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(POSTED_LOG_FILE)
        if blob.exists():
            return json.loads(blob.download_as_string())
    except Exception as e:
        logging.warning(f"Не удалось загрузить журнал публикаций: {e}")
    return {}

def save_posted_log_to_gcs(log):
    try:
        _ensure_gcs_credentials()
        client = _gcs_client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(POSTED_LOG_FILE)
        blob.upload_from_string(
            json.dumps(log, ensure_ascii=False, indent=2),
            content_type="application/json"
        )
        logging.info("Журнал публикаций сохранён в GCS")
    except Exception as e:
        logging.error(f"Ошибка сохранения журнала публикаций: {e}")

def post_to_telegram(title, text):
    """Синхронная отправка через Bot API без async."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHANNEL_ID,
        "text": f"<b>{title}</b>\n\n{text}",
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    r = requests.post(url, json=payload, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Telegram API error {r.status_code}: {r.text}")

def main():
    try:
        data = download_json_from_gcs()
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        current_slot = get_current_slot()

        if not current_slot:
            logging.info("⏳ Текущее время не входит в установленные слоты.")
            return

        today_posts = next((item['posts'] for item in data if item['date'] == today), None)
        if not today_posts:
            logging.warning(f"⚠️ Посты на дату {today} не найдены.")
            return

        slot_index = list(TIME_SLOTS.keys()).index(current_slot)

        if slot_index < len(today_posts):
            posted_log = load_posted_log_from_gcs()

            # уже публиковали?
            if posted_log.get(today, {}).get(current_slot):
                logging.info(f"✅ Пост для {today} в слоте {current_slot} уже был опубликован.")
                return

            post = today_posts[slot_index]
            logging.info(f"📢 Публикация поста: {post['title']}")
            post_to_telegram(post['title'], post['text'])

            # отмечаем публикацию
            posted_log.setdefault(today, {})[current_slot] = True
            save_posted_log_to_gcs(posted_log)
        else:
            logging.info(f"ℹ️ Нет поста для слота {current_slot}")

    except Exception as e:
        logging.exception(f"❌ Ошибка в публикации: {e}")

if __name__ == "__main__":
    main()

