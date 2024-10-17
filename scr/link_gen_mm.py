import pandas as pd
from bs4 import BeautifulSoup
import re

# Параметры
input_file_path = 'Кабинет продавца - Мегамаркета.html'  # Укажите путь к вашему HTML-файлу
output_excel_path = 'products.xlsx'  # Укажите путь для вывода Excel

# Function to extract data from HTML
def extract_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    products = []

    # Adjust the selection according to your HTML structure
    for product_div in soup.find_all(class_='suggested-good'):
        title_tag = product_div.find(class_='suggested-good__title')
        price_tag = product_div.find('span', string=re.compile(r'Цена:\s*'))

        if title_tag and price_tag:
            title = title_tag.get_text(strip=True)
            price_text = price_tag.find_next('span').get_text(strip=True)

            # Clean price
            price_cleaned = ''.join(price_text.split())
            price_numeric = int(price_cleaned.replace('₽', '').replace(' ', ''))

            product_link = title_tag.find('a')['href']
            product_link += '/#?details_block=prices&'

            products.append({
                'Название': title,
                'Цена': price_numeric,
                'Ссылка': product_link
            })
    return products

# Function to save data to Excel
def save_to_excel(products):
    df = pd.DataFrame(products)
    df.to_excel(output_excel_path, index=False, engine='openpyxl')

# Main part of the script
if __name__ == "__main__":
    with open(input_file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()

    products = extract_data(html_content)
    save_to_excel(products)

    print(f"Данные успешно сохранены в {output_excel_path}")




