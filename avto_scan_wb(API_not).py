import os
import requests
from bs4 import BeautifulSoup
import hashlib
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from textblob import TextBlob
import schedule
import time
from telegram import Bot
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import smtplib
from email.mime.text import MIMEText

# Загрузка переменных окружения
load_dotenv()

# --- Параметры из .env ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

DATABASE_URL = os.getenv('DATABASE_URL')

REPORT_EMAIL = os.getenv('REPORT_EMAIL')
EMAIL_SMTP_SERVER = os.getenv('EMAIL_SMTP_SERVER')
EMAIL_SMTP_PORT = int(os.getenv('EMAIL_SMTP_PORT', '587'))
EMAIL_LOGIN = os.getenv('EMAIL_LOGIN')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

# Инициализация Telegram бота
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# Инициализация базы данных
engine = create_engine(DATABASE_URL)
Base = declarative_base()

class Review(Base):
    __tablename__ = 'reviews'
    id = Column(Integer, primary_key=True)
    review_id = Column(String, unique=True, nullable=False)
    source = Column(String, nullable=False)  # Бренд или конкурент
    text = Column(Text, nullable=False)
    sentiment = Column(String, nullable=False)
    date = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Ключевые слова для выявления брака/жалоб
DEFECT_KEYWORDS = ['брак', 'некачественный', 'поломка', 'дефект', 'возврат']

# --- Функция парсинга отзывов с сайта Wildberries ---
def get_reviews_from_wildberries(url, source_name):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0 Safari/537.36"
    }
    reviews = []
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Пример CSS-селекторов для отзывов Wildberries (может потребоваться адаптация)
        review_blocks = soup.select('.feedback__item')  # Основной блок отзыва

        for idx, block in enumerate(review_blocks):
            text_elem = block.select_one('.feedback__text')
            text = text_elem.get_text(strip=True) if text_elem else ''

            date_elem = block.select_one('.feedback__date')
            date_str = date_elem.get_text(strip=True) if date_elem else ''
            try:
                review_date = datetime.strptime(date_str, '%d.%m.%Y')
            except:
                review_date = datetime.utcnow()

            # Уникальный ID - хеш строки отзыва и индекс
            review_id = f"{source_name}_{idx}_{hashlib.md5(text.encode('utf-8')).hexdigest()}"

            reviews.append({
                'id': review_id,
                'text': text,
                'date': review_date.isoformat(),
                'source': source_name
            })

        print(f"[INFO] Получено {len(reviews)} отзывов с {url}")
        return reviews

    except Exception as e:
        print(f"[ERROR] Ошибка парсинга отзывов с {url}: {e}")
        return []

# --- Анализ тональности ---
def analyze_sentiment(text):
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0.1:
        return 'positive'
    elif polarity < -0.1:
        return 'negative'
    else:
        return 'neutral'

# --- Проверка наличия жалоб на брак ---
def contains_defect(text):
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in DEFECT_KEYWORDS)

# --- Сохранение отзывов в базу данных ---
def save_review_to_db(review):
    session = Session()
    try:
        exists = session.query(Review).filter_by(review_id=review['id']).first()
        if not exists:
            date_parsed = review.get('date')
            if isinstance(date_parsed, str):
                try:
                    date_parsed = datetime.fromisoformat(date_parsed)
                except:
                    date_parsed = datetime.utcnow()

            sentiment = analyze_sentiment(review['text'])
            db_review = Review(
                review_id=review['id'],
                source=review['source'],
                text=review['text'],
                sentiment=sentiment,
                date=date_parsed
            )
            session.add(db_review)
            session.commit()
            return db_review
        return None
    except Exception as e:
        print(f"[ERROR] Ошибка сохранения в БД: {e}")
    finally:
        session.close()

# --- Отправка Telegram сообщений ---
def send_telegram_message(message):
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        print("[INFO] Отправлено в Telegram.")
    except Exception as e:
        print(f"[ERROR] Ошибка отправки Telegram сообщения: {e}")

# --- Отправка отчёта по email ---
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

# --- Основной процесс обработки ---
def process_and_store_reviews():
    # URL бренда STILMA на wildberries (пример)
    stilma_url = 'https://www.wildberries.ru/brands/312136445-stilma'
    competitor_url = 'https://www.wildberries.ru/brands/competitor_brand'  # заменить на реальный URL

    stilma_reviews = get_reviews_from_wildberries(stilma_url, 'STILMA')
    competitor_reviews = get_reviews_from_wildberries(competitor_url, 'Competitors')

    defects_found = []

    for review in stilma_reviews:
        saved_review = save_review_to_db(review)
        if saved_review and saved_review.sentiment == 'negative' and contains_defect(saved_review.text):
            defects_found.append(saved_review)

    for review in competitor_reviews:
        save_review_to_db(review)

    for defect in defects_found:
        message = (
            f"⚠️ Жалоба на брак!\n"
            f"ID отзыва: {defect.review_id}\n"
            f"Источник: {defect.source}\n"
            f"Дата: {defect.date.strftime('%Y-%m-%d %H:%M')}\n"
            f"Текст: {defect.text}"
        )
        send_telegram_message(message)

    return stilma_reviews, competitor_reviews

# --- Формирование отчёта ---
def generate_report(period='week'):
    session = Session()
    try:
        end_date = datetime.utcnow()
        if period == 'week':
            start_date = end_date - timedelta(weeks=1)
        elif period == 'month':
            start_date = end_date - timedelta(days=30)
        else:
            start_date = end_date - timedelta(weeks=1)

        stilma_reviews = session.query(Review).filter(
            Review.source == 'STILMA',
            Review.date >= start_date,
            Review.date <= end_date
        ).all()
        competitor_reviews = session.query(Review).filter(
            Review.source == 'Competitors',
            Review.date >= start_date,
            Review.date <= end_date
        ).all()

        def summarize(reviews):
            total = len(reviews)
            pos = sum(r.sentiment == 'positive' for r in reviews)
            neu = sum(r.sentiment == 'neutral' for r in reviews)
            neg = sum(r.sentiment == 'negative' for r in reviews)
            return total, pos, neu, neg

        stilma_total, stilma_pos, stilma_neu, stilma_neg = summarize(stilma_reviews)
        comp_total, comp_pos, comp_neu, comp_neg = summarize(competitor_reviews)

        report = (
            f"📅 Отчёт за период: {start_date.date()} - {end_date.date()}\n"
            f"STILMA: Всего отзывов: {stilma_total}, Позитивных: {stilma_pos}, "
            f"Нейтральных: {stilma_neu}, Негативных: {stilma_neg}\n"
            f"Конкуренты: Всего отзывов: {comp_total}, Позитивных: {comp_pos}, "
            f"Нейтральных: {comp_neu}, Негативных: {comp_neg}\n"
        )
        return report
    finally:
        session.close()

# --- Планировщик ---
def daily_job():
    print(f"[{datetime.utcnow()}] Ежедневная обработка отзывов...")
    process_and_store_reviews()

def weekly_report():
    print(f"[{datetime.utcnow()}] Формирование еженедельного отчёта...")
    report = generate_report('week')
    send_telegram_message(report)
    send_email_report('Еженедельный отчет STILMA', report, REPORT_EMAIL)

def monthly_report():
    print(f"[{datetime.utcnow()}] Формирование ежемесячного отчёта...")
    report = generate_report('month')
    send_telegram_message(report)
    send_email_report('Ежемесячный отчет STILMA', report, REPORT_EMAIL)

schedule.every().day.at("10:00").do(daily_job)
schedule.every().monday.at("10:05").do(weekly_report)
schedule.every().month.at("10:10").do(monthly_report)

if __name__ == "__main__":
    print("[INFO] Запущена система анализа отзывов STILMA на Wildberries (парсинг без API)")
    while True:
        schedule.run_pending()
        time.sleep(60)
