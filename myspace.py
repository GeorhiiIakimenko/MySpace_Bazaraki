import os
import json
import telebot
import time
import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
import re
import requests
from bs4 import BeautifulSoup
import logging
import random
import schedule
from gspread.exceptions import APIError



# Enhanced Logging setup
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("bazaraki_bot.log"),
                        logging.StreamHandler()
                    ])

# Telegram Bot Token
TOKEN = '7296128970:AAEeWFIbtuTu8PCzpJuJMNZP-y80SogpVa0'
bot = telebot.TeleBot(TOKEN)

# Google Sheets setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Получаем содержимое JSON-файла из переменной окружения
service_account_info = json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON', '{}'))

# Используем содержимое для создания учетных данных
creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
client = gspread.authorize(creds)

# Open the Google Spreadsheet
sheet = client.open_by_key('1bzbds-U64u9maqxquzeysUNKgC8dgPbMSHaoEHDnfW8').sheet1

# Используем содержимое для создания учетных данных
creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)

# Если учетные данные устарели, обновляем их
if creds and creds.expired and creds.refresh_token:
    creds.refresh(Request())

client = gspread.authorize(creds)

# Open the Google Spreadsheet
sheet = client.open_by_key('1bzbds-U64u9maqxquzeysUNKgC8dgPbMSHaoEHDnfW8').sheet1


def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()


def create_column_headers():
    headers = ["City", "Price", "Rooms", "Bathrooms", "Area", "URL"]
    if sheet.row_values(1) != headers:
        sheet.insert_row(headers, 1)
        logging.info("Column headers created in Google Sheets")


def get_listing_urls(max_listings=100):
    urls = []
    page = 1
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    logging.info(f"Starting to collect listing URLs (max: {max_listings})")
    while len(urls) < max_listings:
        url = f"https://www.bazaraki.com/real-estate-for-sale/apartments-flats/?price_min=200000&price_max=250000&page={page}"
        logging.info(f"Accessing page {page}: {url}")

        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        listings = soup.select('div.advert__section > a')

        logging.info(f"Found {len(listings)} listings on page {page}")

        if not listings:
            logging.info(f"No more listings found on page {page}. Stopping.")
            break

        for listing in listings:
            listing_url = 'https://www.bazaraki.com' + listing['href']
            urls.append(listing_url)
            logging.info(f"Added URL: {listing_url}")
            if len(urls) == max_listings:
                logging.info(f"Reached maximum number of listings ({max_listings}). Stopping.")
                return urls

        page += 1
        time.sleep(random.uniform(1, 3))  # Добавляем случайную задержку между запросами

    logging.info(f"Collected a total of {len(urls)} listing URLs")
    return urls


def scrape_listing(url):
    logging.info(f"Scraping listing: {url}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Обновленный селектор для города
    city = soup.select_one('span[itemprop="address"]')
    if not city:
        city = soup.select_one('.announcement-meta--single span')
    city = clean_text(city.text) if city else "Not found"
    logging.info(f"City: {city}")

    price = soup.select_one('.announcement-price div')
    price = clean_text(price.text) if price else "Not found"
    logging.info(f"Price: {price}")

    rooms = soup.select_one('li:has(span.key-chars:contains("Bedrooms")) .value-chars')
    rooms = clean_text(rooms.text) if rooms else "Not found"
    logging.info(f"Rooms: {rooms}")

    baths = soup.select_one('li:has(span.key-chars:contains("Bathrooms")) .value-chars')
    baths = clean_text(baths.text) if baths else "Not found"
    logging.info(f"Bathrooms: {baths}")

    area = soup.select_one('li:has(span.key-chars:contains("Property area:"))')
    area = clean_text(area.select_one('.value-chars').text) if area else "Not found"
    logging.info(f"Area: {area}")

    return {
        'city': city,
        'price': price,
        'rooms': rooms,
        'baths': baths,
        'area': area,
        'url': url
    }


def scrape_bazaraki(max_listings=100):
    logging.info(f"Starting Bazaraki scraping process (max listings: {max_listings})")
    apartments = []

    try:
        urls = get_listing_urls(max_listings)

        for index, url in enumerate(urls):
            logging.info(f"Processing listing #{index + 1}/{len(urls)}")
            retries = 3
            while retries > 0:
                try:
                    apartment = scrape_listing(url)
                    if apartment:
                        apartments.append(apartment)
                        logging.info(f"Successfully scraped listing #{index + 1}")
                    else:
                        logging.warning(f"Failed to scrape listing #{index + 1}")
                    break
                except Exception as e:
                    logging.error(f"Error scraping listing {url}: {e}")
                    retries -= 1
                    if retries > 0:
                        logging.info(f"Retrying... ({retries} attempts left)")
                        time.sleep(5)
                    else:
                        logging.error(f"Failed to scrape listing {url} after multiple attempts")

            time.sleep(random.uniform(1, 3))  # Добавляем случайную задержку между запросами

    except Exception as e:
        logging.error(f"Unexpected error during scraping: {e}", exc_info=True)

    logging.info(f"Scraping completed. Total apartments scraped: {len(apartments)}")
    return apartments


def update_sheet(apartments):
    logging.info("Updating Google Sheet")
    create_column_headers()
    existing_links = sheet.col_values(6)[1:]  # Get all URLs at once
    new_apartments = []
    rows_to_append = []

    for apartment in apartments:
        if apartment['url'] not in existing_links:
            rows_to_append.append([
                apartment['city'],
                apartment['price'],
                apartment['rooms'],
                apartment['baths'],
                apartment['area'],
                apartment['url']
            ])
            new_apartments.append(apartment)
            logging.info(f"New apartment found: {apartment['url']}")

    if rows_to_append:
        retries = 3
        while retries > 0:
            try:
                sheet.append_rows(rows_to_append)
                logging.info(f"Added {len(rows_to_append)} new apartments to the sheet")
                break
            except APIError as e:
                if e.response.status_code == 429:
                    logging.warning("API quota exceeded. Waiting before retrying...")
                    time.sleep(60)  # Wait for 60 seconds before retrying
                    retries -= 1
                else:
                    logging.error(f"Error updating sheet: {e}")
                    break
        else:
            logging.error("Failed to update sheet after multiple retries")

    logging.info(f"Updated sheet with {len(new_apartments)} new apartments")
    return new_apartments


def send_telegram_notifications(chat_id, new_apartments):
    logging.info(f"Sending Telegram notifications for {len(new_apartments)} apartments")
    if not new_apartments:
        bot.send_message(chat_id=chat_id, text="No new real estate objects found.")
        logging.info("No new apartments to notify about")
        return

    for apartment in new_apartments:
        message = f"""
New real estate object:
City: {apartment['city']}
Price: {apartment['price']}
Rooms: {apartment['rooms']}
Bathrooms: {apartment['baths']}
Area: {apartment['area']}
Link: {apartment['url']}
"""
        try:
            bot.send_message(chat_id=chat_id, text=message)
            logging.info(f"Sent notification for apartment: {apartment['url']}")
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error sending message: {e}", exc_info=True)


def periodic_check():
    logging.info("Starting periodic check")
    apartments = scrape_bazaraki(max_listings=100)
    if apartments:
        new_apartments = update_sheet(apartments)
        if new_apartments:
            logging.info(f"Found {len(new_apartments)} new objects out of {len(apartments)} checked.")
            # Здесь вы можете добавить код для отправки уведомлений, если это необходимо
        else:
            logging.info(f"No new real estate objects found out of {len(apartments)} checked.")
    else:
        logging.error("Failed to retrieve real estate information.")
    logging.info("Periodic check completed")


@bot.message_handler(commands=['start'])
def handle_start(message):
    logging.info(f"Received /start command from user {message.from_user.id}")
    bot.reply_to(message, "Starting the search for real estate objects (checking up to 100 listings)...")
    apartments = scrape_bazaraki(max_listings=100)
    if apartments:
        new_apartments = update_sheet(apartments)
        if new_apartments:
            bot.reply_to(message,
                         f"Found {len(new_apartments)} new objects out of {len(apartments)} checked. Sending information...")
            send_telegram_notifications(message.chat.id, new_apartments)
        else:
            bot.reply_to(message, f"No new real estate objects found out of {len(apartments)} checked.")
    else:
        bot.reply_to(message, "Failed to retrieve real estate information. Please try again later.")
    bot.reply_to(message, "Search completed. The bot will now check for updates every 10 hours.")
    logging.info("Completed processing /start command")


def run_scheduled_checks():
    schedule.every(10).hours.do(periodic_check)
    while True:
        schedule.run_pending()
        time.sleep(1)


def run_bot():
    import threading
    logging.info("Starting scheduled checks")
    check_thread = threading.Thread(target=run_scheduled_checks)
    check_thread.start()

    while True:
        try:
            logging.info("Starting bot polling...")
            bot.polling(none_stop=True, timeout=60, long_polling_timeout=30)
        except Exception as e:
            logging.error(f"Error in bot polling: {e}", exc_info=True)
            time.sleep(15)


if __name__ == "__main__":
    logging.info("Starting Bazaraki Real Estate Bot (100 listings, checks every 10 hours)")
    run_bot()