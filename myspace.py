import telebot
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from bs4 import BeautifulSoup
import logging
import random
import schedule
import threading

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
SERVICE_ACCOUNT_FILE = 'boxwood-chalice-422009-j8-02152d6b48e7.json'
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
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

def setup_driver():
    logging.info("Setting up Chrome driver")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)
    logging.info("Chrome driver setup completed")
    return driver

def get_listing_urls(driver, max_listings=None):
    urls = []
    page = 1
    logging.info(f"Starting to collect listing URLs (max: {max_listings})")
    while len(urls) < (max_listings or float('inf')):
        url = f"https://www.bazaraki.com/real-estate-for-sale/apartments-flats/?price_min=200000&price_max=250000&page={page}"
        logging.info(f"Accessing page {page}: {url}")
        driver.get(url)

        delay = random.uniform(2, 5)
        logging.info(f"Waiting for {delay:.2f} seconds")
        time.sleep(delay)

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.advert__section > a'))
            )
        except TimeoutException:
            logging.error(f"Timeout waiting for listings on page {page}")
            break

        listings = driver.find_elements(By.CSS_SELECTOR, 'div.advert__section > a')
        logging.info(f"Found {len(listings)} listings on page {page}")

        if not listings:
            logging.info(f"No more listings found on page {page}. Stopping.")
            break

        for listing in listings:
            try:
                listing_url = listing.get_attribute('href')
                urls.append(listing_url)
                logging.info(f"Added URL: {listing_url}")
                if len(urls) == max_listings:
                    logging.info(f"Reached maximum number of listings ({max_listings}). Stopping.")
                    break
            except StaleElementReferenceException:
                logging.warning("Encountered a stale element. Skipping.")
                continue

        page += 1

    logging.info(f"Collected a total of {len(urls)} listing URLs")
    return urls[:max_listings]

def scrape_listing(driver, url):
    logging.info(f"Scraping listing: {url}")
    driver.get(url)

    delay = random.uniform(2, 5)
    logging.info(f"Waiting for {delay:.2f} seconds")
    time.sleep(delay)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.announcement-content-container'))
        )
    except TimeoutException:
        logging.error(f"Timeout waiting for content on {url}")
        return None

    soup = BeautifulSoup(driver.page_source, 'html.parser')

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

def scrape_bazaraki(max_listings=200):
    logging.info(f"Starting Bazaraki scraping process (max listings: {max_listings})")
    driver = setup_driver()
    apartments = []

    try:
        urls = get_listing_urls(driver, max_listings)

        for index, url in enumerate(urls):
            logging.info(f"Processing listing #{index + 1}/{len(urls)}")
            apartment = scrape_listing(driver, url)
            if apartment:
                apartments.append(apartment)
                logging.info(f"Successfully scraped listing #{index + 1}")
            else:
                logging.warning(f"Failed to scrape listing #{index + 1}")

    except Exception as e:
        logging.error(f"Unexpected error during scraping: {e}", exc_info=True)
    finally:
        driver.quit()
        logging.info(f"Scraping completed. Total apartments scraped: {len(apartments)}")

    return apartments

def update_sheet(apartments):
    logging.info("Updating Google Sheet")
    create_column_headers()
    existing_links = sheet.col_values(6)[1:]
    new_apartments = []

    for apartment in apartments:
        if apartment['url'] not in existing_links:
            sheet.append_row([
                apartment['city'],
                apartment['price'],
                apartment['rooms'],
                apartment['baths'],
                apartment['area'],
                apartment['url']
            ])
            new_apartments.append(apartment)
            logging.info(f"Added new apartment: {apartment['url']}")

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
    apartments = scrape_bazaraki(max_listings=200)
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
    bot.reply_to(message, "Starting the search for real estate objects (checking up to 200 listings)...")
    apartments = scrape_bazaraki(max_listings=200)
    if apartments:
        new_apartments = update_sheet(apartments)
        if new_apartments:
            bot.reply_to(message, f"Found {len(new_apartments)} new objects out of {len(apartments)} checked. Sending information...")
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
    logging.info("Starting Bazaraki Real Estate Bot (200 listings, checks every 10 hours)")
    run_bot()
