import os
import requests
import pandas as pd
from dotenv import load_dotenv
from textblob import TextBlob
import schedule
import time
from datetime import datetime, timedelta
from telegram import Bot
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import smtplib
from email.mime.text import MIMEText

# ========== Загрузка переменных окружения ==========
load_dotenv()

API_KEY_STILMA = os.getenv('API_KEY_STILMA')
API_URL_STILMA = os.getenv('API_URL_STILMA')

API_KEY_COMPETITORS = os.getenv('API_KEY_COMPETITORS')
API_URL_COMPETITORS = os.getenv('API_URL_COMPETITORS')

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

DATABASE_URL = os.getenv('DATABASE_URL')

REPORT_EMAIL = os.getenv('REPORT_EMAIL')
EMAIL_SMTP_SERVER = os.getenv('EMAIL_SMTP_SERVER')
EMAIL_SMTP_PORT = int(os.getenv('EMAIL_SMTP_PORT', '587'))
EMAIL_LOGIN = os.getenv('EMAIL_LOGIN')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')

# ========== Инициализация Telegram бота ==========
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# ========== Инициализация базы данных ==========
engine = create_engine(DATABASE_URL)
Base = declarative_base()

class Review(Base):
    __tablename__ = 'reviews'
    id = Column(Integer, primary_key=True)
    review_id = Column(String, unique=True, nullable=False)
    source = Column(String, nullable=False)
    text = Column(Text, nullable=False)
    sentiment = Column(String, nullable=False)
    date = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# ========== Ключевые слова для выявления брака ==========
DEFECT_KEYWORDS = ['брак', 'некачественный', 'поломка', 'дефект', 'возврат']

# ========== Получение отзывов из API маркетплейса ==========
def get_reviews(api_url, api_key, source_name, params=None):
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        response = requests.get(api_url, headers=headers, params=params or {}, timeout=10)
        response.raise_for_status()
        data = response.json()
        # Подстройте под фактическую структуру ответа API
        reviews_raw = data.get('reviews') or data.get('data') or []
        reviews = []
        for r in reviews_raw:
            reviews.append({
                'id': str(r.get('id') or r.get('reviewId') or r.get('review_id')),  # уникальный id от API
                'text': r.get('text') or r.get('comment') or '',
                'date': r.get('date') or r.get('created_at') or datetime.utcnow().isoformat(),
                'source': source_name
            })
        return reviews
    except Exception as e:
        print(f"Ошибка получения отзывов от {source_name}: {e}")
        return []

# ========== Анализ тональности текста ==========
def analyze_sentiment(text):
    analysis = TextBlob(text)
    polarity = analysis.sentiment.polarity
    if polarity > 0.1:
        return 'positive'
    elif polarity < -0.1:
        return 'negative'
    else:
        return 'neutral'

# ========== Проверка на наличие брака ==========
def contains_defect(text):
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in DEFECT_KEYWORDS)

# ========== Сохранение нового отзыва в БД ==========
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
        else:
            return None
    except Exception as e:
        print("Ошибка сохранения в БД:", e)
    finally:
        session.close()

# ========== Отправка сообщений в Telegram ==========
def send_telegram_message(message):
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        print("Отправлено в Telegram.")
    except Exception as e:
        print(f"Ошибка отправки Telegram сообщения: {e}")

# ========== Отправка отчёта по email ==========
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
        print("Отчёт отправлен по email.")
    except Exception as e:
        print(f"Ошибка отправки email: {e}")

# ========== Обработка отзывов, сохранение, выявление жалоб ==========
def process_and_store_reviews():
    stilma_reviews = get_reviews(API_URL_STILMA, API_KEY_STILMA, 'STILMA')
    competitor_reviews = get_reviews(API_URL_COMPETITORS, API_KEY_COMPETITORS, 'Competitors')

    defects_found = []

    # Сохранение STILMA отзывов
    for review in stilma_reviews:
        saved_review = save_review_to_db(review)
        if saved_review and saved_review.sentiment == 'negative' and contains_defect(saved_review.text):
            defects_found.append(saved_review)

    # Сохранение отзывов конкурентов
    for review in competitor_reviews:
        save_review_to_db(review)

    # Отправка жалоб на брак в Telegram
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

# ========== Формирование отчёта ==========
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
            f"STILMA: Всего отзывов: {stilma_total}, Позитивных: {stilma_pos}, Нейтральных: {stilma_neu}, Негативных: {stilma_neg}\n"
            f"Конкуренты: Всего отзывов: {comp_total}, Позитивных: {comp_pos}, Нейтральных: {comp_neu}, Негативных: {comp_neg}\n"
        )
        return report

    except Exception as e:
        print("Ошибка формирования отчёта:", e)
        return ""
    finally:
        session.close()

# ========== Ежедневная задача ==========
def daily_job():
    print(f"[{datetime.utcnow()}] Запуск ежедневной обработки отзывов...")
    process_and_store_reviews()

# ========== Еженедельный отчёт ==========
def weekly_report():
    print(f"[{datetime.utcnow()}] Формирование еженедельного отчёта...")
    report = generate_report('week')
    send_telegram_message(report)
    send_email_report('Еженедельный отчёт STILMA', report, REPORT_EMAIL)

# ========== Ежемесячный отчёт ==========
def monthly_report():
    print(f"[{datetime.utcnow()}] Формирование ежемесячного отчёта...")
    report = generate_report('month')
    send_telegram_message(report)
    send_email_report('Ежемесячный отчёт STILMA', report, REPORT_EMAIL)

# ========== Планировщик ==========
schedule.every().day.at("10:00").do(daily_job)
schedule.every().monday.at("10:05").do(weekly_report)
schedule.every().month.at("10:10").do(monthly_report)

if __name__ == "__main__":
    print("Запущена система анализа отзывов STILMA.")
    while True:
        schedule.run_pending()
        time.sleep(60)
