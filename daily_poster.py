import os
import json
import datetime
import logging

from dotenv import load_dotenv
from google.cloud import storage
from telegram import Bot

# === Загрузка переменных окружения ===
load_dotenv()
logging.basicConfig(level=logging.INFO)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_FILE_NAME = os.getenv("GCS_FILE_NAME")
GCS_CREDENTIALS_JSON = os.getenv("GCS_CREDENTIALS_JSON")

# === Инициализация Telegram бота ===
bot = Bot(token=TELEGRAM_TOKEN)

# === Временные слоты (утро, день, вечер, ночь) ===
TIME_SLOTS = {
    'morning': range(5, 9),     # 05:00–08:59
    'day': range(9, 13),        # 09:00–12:59
    'afternoon': range(13, 17), # 13:00–16:59
    'evening': range(17, 23)    # 17:00–22:59
}

def get_current_slot():
    hour = datetime.datetime.now().hour
    for slot, hours in TIME_SLOTS.items():
        if hour in hours:
            return slot
    return None

def download_json_from_gcs():
    storage_client = storage.Client.from_service_account_json(GCS_CREDENTIALS_JSON)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GCS_FILE_NAME)
    content = blob.download_as_string()
    return json.loads(content)

def post_to_telegram(title, text):
    message = f"<b>{title}</b>\n\n{text}"
    bot.send_message(chat_id=CHANNEL_ID, text=message, parse_mode='HTML')

def main():
    try:
        data = download_json_from_gcs()
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        current_slot = get_current_slot()

        if not current_slot:
            logging.info("Текущее время не входит в установленные слоты.")
            return

        today_posts = next((item['posts'] for item in data if item['date'] == today), None)

        if not today_posts:
            logging.warning(f"Посты на дату {today} не найдены.")
            return

        slot_index = list(TIME_SLOTS.keys()).index(current_slot)

        if slot_index < len(today_posts):
            post = today_posts[slot_index]
            logging.info(f"Публикация поста: {post['title']}")
            post_to_telegram(post['title'], post['text'])
        else:
            logging.info(f"Нет поста для слота {current_slot}")

    except Exception as e:
        logging.exception(f"Ошибка в публикации: {e}")

if __name__ == "__main__":
    main()
