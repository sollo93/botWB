import os
import requests
import hashlib
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from textblob import TextBlob
import schedule
import time
from telegram import Bot
import smtplib
from email.mime.text import MIMEText

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ========== Загрузка переменных окружения ==========
load_dotenv()

# --- Telegram настройки ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# --- Email (Яндекс) настройки ---
EMAIL_SMTP_SERVER = 'smtp.yandex.ru'
EMAIL_SMTP_PORT = 587

EMAIL_LOGIN = os.getenv('EMAIL_LOGIN')      # Ваш логин Яндекс.Почты
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')  # Пароль приложения

EMAIL_RECIPIENT = os.getenv('EMAIL_RECIPIENT')  # Кому отправлять отчёт (ваш email)

# --- Google Drive ---
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')  # Путь к JSON сервисного аккаунта
GDRIVE_FOLDER_ID = os.getenv('GDRIVE_FOLDER_ID')  # ID папки для загрузки (можно оставить пустым)

# --- Ключевые слова брака/дефектов ---
DEFECT_KEYWORDS = ['брак', 'некачественный', 'поломка', 'дефект', 'возврат']

# --- Инициализация Telegram бота ---
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# --- Инициализация Google Drive API ---
SCOPES = ['https://www.googleapis.com/auth/drive.file']

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)
drive_service = build('drive', 'v3', credentials=credentials)

# --- Функция загрузки файла в Google Drive ---
def upload_report_to_gdrive(file_path, folder_id=None):
    file_metadata = {'name': os.path.basename(file_path)}
    if folder_id:
        file_metadata['parents'] = [folder_id]

    media = MediaFileUpload(file_path, mimetype='text/plain', resumable=True)

    try:
        file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        print(f"[INFO] Файл загружен в Google Drive с ID: {file.get('id')}")
        return file.get('id')
    except Exception as e:
        print(f"[ERROR] Ошибка загрузки файла в Google Drive: {e}")
        return None

# --- Получение отзывов Wildberries (по product_id) через AJAX ---
def get_reviews_wb(product_id, max_pages=5):
    reviews = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
    }

    for page in range(1, max_pages + 1):
        url = f"https://card.wb.ru/cards/detail?nm={product_id}&page={page}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            reviews_data = data.get('data', {}).get('orders', {}).get('data', [])
            if not reviews_data:
                break

            for r in reviews_data:
                text = r.get('reviewText', '').strip()
                if not text:
                    continue

                review_id = f"wb_{product_id}_{r.get('reviewId', '')}"
                date_str = r.get('dateCreated')
                try:
                    review_date = datetime.fromisoformat(date_str)
                except:
                    review_date = datetime.utcnow()

                reviews.append({
                    'id': review_id,
                    'product_id': str(product_id),
                    'text': text,
                    'date': review_date,
                    'source': 'wildberries'
                })

            # Если на странице меньше 10 отзывов — возможно последний сканируемый набор
            if len(reviews_data) < 10:
                break

        except Exception as e:
            print(f"[ERROR] Ошибка получения отзывов товара {product_id} страница {page}: {e}")
            break

    print(f"[INFO] Собрано {len(reviews)} отзывов для товара {product_id}")
    return reviews

# --- Анализ тональности ---
def analyze_sentiment(text):
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0.1:
        return 'positive'
    elif polarity < -0.1:
        return 'negative'
    else:
        return 'neutral'

# --- Проверка брака/дефекта в отзыве ---
def contains_defect(text):
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in DEFECT_KEYWORDS)

# --- Отправка сообщения в Telegram ---
def send_telegram_message(message):
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        print("[INFO] Отправлено в Telegram.")
    except Exception as e:
        print(f"[ERROR] Ошибка отправки в Telegram: {e}")

# --- Отправка письма по email через Яндекс ---
def send_email_report(subject, body, recipient):
    try:
        msg = MIMEText(body, 'plain', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = EMAIL_LOGIN
        msg['To'] = recipient

        with smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_LOGIN, EMAIL_PASSWORD)
            server.sendmail(EMAIL_LOGIN, recipient, msg.as_string())
        print("[INFO] Отчёт отправлен по email.")
    except Exception as e:
        print(f"[ERROR] Ошибка отправки email: {e}")

# --- Список товаров (артикулы) для мониторинга ---
PRODUCTS = [
    306924358,
    396066853,
    396226161,
    306929853,
    306927225
]

# --- Основной процесс: сбор, анализ, уведомления ---
def process_and_collect_reviews():
    all_reviews = []
    defects_found = []

    for product_id in PRODUCTS:
        reviews = get_reviews_wb(product_id)
        for r in reviews:
            sentiment = analyze_sentiment(r['text'])
            r['sentiment'] = sentiment
            all_reviews.append(r)
            if sentiment == 'negative' and contains_defect(r['text']):
                defects_found.append(r)
    return all_reviews, defects_found

# --- Формирование текстового отчёта ---
def generate_report(all_reviews):
    total = len(all_reviews)
    positive = sum(r['sentiment'] == 'positive' for r in all_reviews)
    neutral = sum(r['sentiment'] == 'neutral' for r in all_reviews)
    negative = sum(r['sentiment'] == 'negative' for r in all_reviews)

    report = (
        f"📅 Отчёт по отзывам Wildberries (текущий запуск):\n"
        f"Всего отзывов: {total}\n"
        f"Позитивных: {positive}\n"
        f"Нейтральных: {neutral}\n"
        f"Негативных: {negative}\n"
    )
    return report

# --- Задача ежедневной обработки ---
def daily_job():
    print(f"[{datetime.utcnow()}] Запуск ежедневной обработки отзывов Wildberries...")
    all_reviews, defects = process_and_collect_reviews()

    # Отправка тревог по браку в Telegram
    for d in defects:
        message = (
            f"⚠️ Жалоба на брак!\n"
            f"ID: {d['id']}\n"
            f"Товар: {d['product_id']}\n"
            f"Дата: {d['date'].strftime('%Y-%m-%d %H:%M') if isinstance(d['date'], datetime) else d['date']}\n"
            f"Текст: {d['text']}"
        )
        send_telegram_message(message)

    # Формирование и отправка отчёта
    report = generate_report(all_reviews)

    send_telegram_message(report)
    send_email_report('Ежедневный отчет Wildberries', report, EMAIL_RECIPIENT)

    # Сохранение отчёта в файл и загрузка на Google Drive
    filename = f"wildberries_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(report)

    upload_report_to_gdrive(filename, GDRIVE_FOLDER_ID)

# --- Задача еженедельного отчёта (можно просто запускать daily_job) ---
def weekly_report():
    print(f"[{datetime.utcnow()}] Запуск еженедельного отчёта...")
    daily_job()

# --- Задача ежемесячного отчёта с проверкой даты ---
def monthly_report():
    today = date.today()
    if today.day == 1:
        print(f"[{datetime.utcnow()}] Запуск ежемесячного отчёта...")
        daily_job()
    else:
        print(f"[{datetime.utcnow()}] Сегодня не первый день месяца — ежемесячный отчёт пропущен.")

# --- Планировщик ---
schedule.every().day.at("10:00").do(daily_job)
schedule.every().monday.at("10:05").do(weekly_report)
schedule.every().day.at("10:10").do(monthly_report)  # Запускается каждый день, внутри проверяется дата

if __name__ == "__main__":
    print("[INFO] Запущен скрипт мониторинга и обработки отзывов Wildberries.")
    while True:
        schedule.run_pending()
        time.sleep(60)
