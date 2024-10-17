from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
from .logger import logger
import re
import random



my_markets = ['ByMarket','Tech PC Components','SSmart shop','E-Shopper']

loger = logger

def get_random_user_agent():
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
    ]
    return random.choice(user_agents)

def add_random_actions(driver):
    # Прокрутка страницы
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
    time.sleep(random.uniform(1, 3))
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(random.uniform(1, 3))
    driver.execute_script("window.scrollTo(0, 0);")

def get_product_offers(url_dict, logger):
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-extensions")
    options.add_argument("--headless")
    options.add_argument(f"user-agent={get_random_user_agent()}")

    logger.info("Инициализация драйвера")
    try:
        service = Service(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=options)
        logger.info("Драйвер успешно инициализирован")
    except Exception as e:
        logger.error(f"Ошибка при инициализации драйвера: {e}")
        return None

    all_html_content = {}

    try:
        for key, url in url_dict.items():
            logger.info(f"Попытка загрузки страницы: {url}")
            driver.get(url)
            logger.debug("Команда загрузки страницы выполнена")

            # Добавление случайных действий
            add_random_actions(driver)

            wait = WebDriverWait(driver, 60)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            logger.debug("Элемент <body> загружен")

            try:
                wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
                logger.debug("Страница полностью загружена")

                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".product-offer, .product-not-found")))

                product_offer_exists = driver.execute_script("return document.querySelector('.product-offer') !== null")
                if product_offer_exists:
                    logger.info("Элемент product-offer найден")
                else:
                    logger.warning("Элемент product-offer не найден")

            except TimeoutException:
                logger.warning("Превышено время ожидания загрузки страницы")

            html_content = driver.page_source
            logger.debug("HTML страницы получен")
            all_html_content[key] = html_content

            # Случайная задержка между запросами
            time.sleep(random.uniform(5, 10))

    except WebDriverException as e:
        logger.error(f"Ошибка WebDriver: {e}")
    except Exception as e:
        logger.error(f"Произошла неожиданная ошибка: {e}")
    finally:
        logger.info("Закрытие браузера")
        driver.quit()

    return all_html_content


def extract_data(html_content, logger):
    soup = BeautifulSoup(html_content, 'html.parser')
    offers = soup.find_all('div', class_='product-offer')

    if not offers:
        logger.warning("Элементы product-offer не найдены. Проверка альтернативных элементов.")
        # Здесь можно добавить проверку на наличие альтернативных элементов
        # Например, элемент, указывающий на отсутствие товара

    data = []
    for offer in offers:
        seller_name = offer.find('span', class_='pdp-merchant-rating-block__merchant-name')
        seller_name = seller_name.text if seller_name else "N/A"

        current_price = offer.find('span', class_='product-offer-price__amount')
        current_price = current_price.text.strip() if current_price else "N/A"

        data.append({
            'market_with_mp': seller_name,
            'mp_on_market': current_price
        })

    if not data:
        logger.warning("Не удалось извлечь данные о предложениях.")
        return None

    df = pd.DataFrame(data)

    if 'mp_on_market' in df.columns:
        df['mp_on_market'] = df['mp_on_market'].apply(
            lambda x: float(re.sub(r'[^\d.]', '', x)) if x != "N/A" else np.nan)

    return df


def scrape_megamarket(input_df):
    logger = loger
    logger.info("Начало выполнения функции scrape_megamarket")

    input_df = input_df.rename(columns={
        'ART': 'seller_id',
        'Product Name': 'name',
        'URL': 'link'
    })

    product_info = dict(zip(input_df['name'], zip(input_df['link'], input_df['seller_id'])))
    url_dict = {name: info[0] for name, info in product_info.items()}

    html_contents = get_product_offers(url_dict, logger)

    result_data = []

    if html_contents:
        for product_name, html_content in html_contents.items():
            df = extract_data(html_content, logger)
            if df is not None and not df.empty:
                logger.info(f"Данные извлечены для {product_name}")
                df['name'] = product_name
                df['seller_id'] = product_info[product_name][1]
                result_data.append(df)
            else:
                logger.warning(f"Не удалось извлечь данные для {product_name}.")
                # Добавляем пустую строку для продуктов без данных
                result_data.append(pd.DataFrame({
                    'name': [product_name],
                    'seller_id': [product_info[product_name][1]],
                    'mp_on_market': [np.nan],
                    'market_with_mp': ['N/A']
                }))

    if result_data:
        result_df = pd.concat(result_data, ignore_index=True)

        # Находим минимальную цену для каждого уникального seller_id
        min_price_df = result_df.groupby('seller_id', as_index=False).apply(
            lambda x: x.loc[x['mp_on_market'].idxmin() if not x['mp_on_market'].isna().all() else x.index[0]]
        )

        # Оставляем только нужные колонки в нужном порядке
        final_df = min_price_df[['seller_id', 'name', 'mp_on_market', 'market_with_mp']]

        logger.info("Скрипт успешно завершил работу")
        return final_df
    else:
        logger.error("Не удалось получить данные ни для одного продукта")
        return pd.DataFrame()


def extract_data(html_content, logger):
    soup = BeautifulSoup(html_content, 'html.parser')
    offers = soup.find_all('div', class_='product-offer')

    data = []
    for offer in offers:
        seller_name = offer.find('span', class_='pdp-merchant-rating-block__merchant-name')
        seller_name = seller_name.text if seller_name else "N/A"

        current_price = offer.find('span', class_='product-offer-price__amount')
        current_price = current_price.text.strip() if current_price else "N/A"

        data.append({
            'market_with_mp': seller_name,
            'mp_on_market': current_price
        })

    if not data:
        logger.warning("Не удалось извлечь данные о предложениях.")
        return None

    df = pd.DataFrame(data)

    if 'mp_on_market' in df.columns:
        df['mp_on_market'] = df['mp_on_market'].apply(
            lambda x: float(re.sub(r'[^\d.]', '', x)) if x != "N/A" else np.nan)

    return df


def scrape_megamarket(input_df):
    logger = loger
    logger.info("Начало выполнения функции scrape_megamarket")

    # Переименовываем колонки входного датафрейма
    input_df = input_df.rename(columns={
        'ART': 'seller_id',
        'Product Name': 'name',
        'URL': 'link'
    })

    # Создаем словарь, где ключ - это название продукта, а значение - кортеж (URL, seller_id)
    product_info = dict(zip(input_df['name'], zip(input_df['link'], input_df['seller_id'])))

    # Создаем словарь только с URL для функции get_product_offers
    url_dict = {name: info[0] for name, info in product_info.items()}

    html_contents = get_product_offers(url_dict, logger)

    result_data = []

    if html_contents:
        for product_name, html_content in html_contents.items():
            df = extract_data(html_content, logger)
            if df is not None:
                logger.info(f"Данные извлечены для {product_name}")
                df['name'] = product_name
                df['seller_id'] = product_info[product_name][1]  # Добавляем seller_id
                result_data.append(df)
            else:
                logger.warning(f"Не удалось извлечь данные для {product_name}.")

    if result_data:
        result_df = pd.concat(result_data, ignore_index=True)
        result_df = result_df[~result_df['market_with_mp'].isin(my_markets)]

        # Находим минимальную цену для каждого уникального seller_id
        min_price_df = result_df.groupby('seller_id').apply(
            lambda x: x.loc[x['mp_on_market'].idxmin()]
        ).reset_index(drop=True)

        # Оставляем только нужные колонки в нужном порядке
        final_df = min_price_df[['seller_id', 'name', 'mp_on_market', 'market_with_mp']]

        logger.info("Скрипт успешно завершил работу")
        return final_df
    else:
        logger.error("Не удалось получить данные ни для одного продукта")
        return pd.DataFrame()