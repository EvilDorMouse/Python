import os
from dotenv import load_dotenv
from mysql.connector import Error, pooling
from selenium.common.exceptions import WebDriverException
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from multiprocessing import Process, Queue
import logging
from openai import OpenAI
from contextlib import contextmanager

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log'),
        logging.StreamHandler()
    ]
)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
CONFIG = {
    'DB_HOST': os.getenv('DB_HOST', 'localhost'),
    'DB_USER': os.getenv('DB_USER'),
    'DB_PASSWORD': os.getenv('DB_PASSWORD'),
    'DB_NAME': os.getenv('DB_NAME', 'test'),
    'MAX_THREADS': int(os.getenv('MAX_THREADS', 4)),
    'BATCH_SIZE': int(os.getenv('BATCH_SIZE', 400)),
    'OPENAI_API_KEY': os.getenv('OPENAI_API_KEY'),
    'PROXY': os.getenv('PROXY')
}

# SQL-запрос для обработки строк в БД
QUERIES = {
    'select_company': """
        SELECT id, homepageurl 
        FROM company 
        WHERE description = '' 
        LIMIT %s
    """,
    'update_company': """
        UPDATE company 
        SET description = %s 
        WHERE id = %s
    """
}

# Ключевые слова и данные (для БД)
KEYWORDS = {
    'basic': ['Description', 'Target Audience', 'Market problems',
              'Product Description', 'Business model', 'Industries'],
    'complex': {
        'Market problems': 'Problem',
        'Product Description': 'Product',
        'Industries': 'Industry'
    },
    'business_models': ['B2B', 'B2C', 'B2G', 'C2C'],
    'industries': [
        'Administrative Services',
        'Advertising',
        'Agriculture & Farming',
        'Apps',
        'Artificial Intelligence',
        'Biotechnology',
        'Clothing & Apparel',
        'Commerce & Shopping',
        'Community & Lifestyle',
        'Consumer Electronics',
        'Consumer Goods',
        'Content and Publishing',
        'Data & Analytics',
        'Design',
        'Education',
        'Energy',
        'Events',
        'Financial Services',
        'Food and Beverage',
        'Gaming',
        'Government and Military',
        'Hardware',
        'Health Care',
        'Information Technology',
        'Internet Services',
        'Lending and Investments',
        'Manufacturing',
        'Media and Entertainment',
        'Messaging and Telecommunications',
        'Mobile',
        'Music and Audio',
        'Natural Resources',
        'Navigation and Mapping',
        'Payments',
        'Platforms',
        'Privacy and Security',
        'Professional Services',
        'Real Estate',
        'Sales and Marketing',
        'Science and Engineering',
        'Software',
        'Sports',
        'Sustainability',
        'Transportation',
        'Travel and Tourism',
        'Video',
    ]
}


@contextmanager
def get_db_connection():
    try:
        connection_pool = pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=5,
            host=CONFIG['DB_HOST'],
            user=CONFIG['DB_USER'],
            password=CONFIG['DB_PASSWORD'],
            database=CONFIG['DB_NAME']
        )
        conn = connection_pool.get_connection()
        yield conn
    except Error as e:
        logging.error(f"Database error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


@contextmanager
def get_webdriver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = None
    try:
        driver = webdriver.Chrome(options=options)
        yield driver
    except WebDriverException as e:
        logging.error(f"WebDriver error: {e}")
        raise
    finally:
        if driver:
            driver.quit()


# Получение списка ссылок
def fetch_companies():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(QUERIES['select_company'], (CONFIG['BATCH_SIZE'],))
                return cursor.fetchall()
    except Error as e:
        logging.error(f"Error fetching companies: {e}")
        return []


# Парсинг данных о компании
def parse_company(company_data, result_queue):
    company_id, url = company_data
    try:
        with get_webdriver() as driver:
            driver.get(url)

            WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.TAG_NAME, 'body'))
            )

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Сбор текста
            elements = driver.find_elements(By.XPATH, '//div | //p')
            text = ' '.join(el.text for el in elements if el.text.strip())

            # Очистка текста
            text = text.replace('"', '').replace("'", '').strip()

            result_queue.put((text, company_id))
            logging.info(f"Processed company ID: {company_id}")

    except Exception as e:
        logging.error(f"Error processing {url}: {e}")
        result_queue.put(("ERROR", company_id))


# Обработка текста с помощью ChatGPT
def process_with_chatgpt(text):
    if not text or text == "ERROR":
        return None

    try:
        client = OpenAI(api_key=CONFIG['OPENAI_API_KEY'])

        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system",
                 "content": "You are a helpful assistant that provides concise business descriptions."},
                {"role": "user", "content": f"""
                Analyze this company information and provide:
                #Description# - Brief company overview
                #Target Audience# - Who they serve
                #Market Problem# - Problem they solve
                #Product# - Their product/service
                #Business Model# - One of: {', '.join(KEYWORDS['business_models'])}
                #Industry# - One of: {', '.join(KEYWORDS['industries'][:5])}...
                Text to analyze: {text[:3000]}
                """}
            ],
            max_tokens=500
        )

        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"ChatGPT error: {e}")
        return None


# Запись результатов в БД
def save_results(result_queue):
    while True:
        item = result_queue.get()
        if item == "STOP":
            break

        text, company_id = item

        # Обработка через ChatGPT
        # processed_text = process_with_chatgpt(text)
        processed_text = text  # Заглушка

        if processed_text:
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(QUERIES['update_company'], (processed_text, company_id))
                        conn.commit()
                        logging.info(f"Updated company ID: {company_id}")
            except Error as e:
                logging.error(f"Error saving company {company_id}: {e}")


def main():
    companies = fetch_companies()
    if not companies:
        logging.warning("No companies to process")
        return

    result_queue = Queue()

    # Сохранение результатов
    saver_process = Process(target=save_results, args=(result_queue,))
    saver_process.start()

    # Обработка в несколько параллельных процессов
    processes = []
    for company in companies:
        p = Process(target=parse_company, args=(company, result_queue))
        processes.append(p)
        p.start()

        # Ограничение количества одновременных процессов
        if len(processes) >= CONFIG['MAX_THREADS']:
            for p in processes:
                p.join()
            processes = []

    # Ожидание оставшихся процессов
    for p in processes:
        p.join()

    result_queue.put("STOP")
    saver_process.join()

    logging.info("Processing completed")


if __name__ == "__main__":
    main()
