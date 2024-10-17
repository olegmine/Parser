
# MegaMarket Updater

Этот проект представляет собой асинхронное приложение для автоматического обновления данных о товарах на маркетплейсе MegaMarket.

## Особенности

- Асинхронное выполнение для эффективной обработки данных
- Интеграция с Google Sheets для получения и обновления данных
- Скрапинг данных с MegaMarket
- Обновление цен через API MegaMarket
- Логирование процессов и ошибок
- Режим отладки с сохранением промежуточных результатов в CSV

## Требования

- Python 3.7+
- pandas
- aiohttp
- google-auth
- google-auth-oauthlib
- google-auth-httplib2
- google-api-python-client

## Установка

1. Клонируйте репозиторий:
   ```
   https://github.com/olegmine/Parser
   cd Parser
   ```
2. Создайте и активируйте виртуальное окружение :
   ```
   python3 -m venv myenv
   ```
   ```
   source myenv/bin/activate    
   ```


    

   
   

3. Установите зависимости:
   ```
   pip install -r requirements.txt
   ```

4. Настройте файл конфигурации `config.py` с вашими данными:
   - ID Google Spreadsheet
   - API ключи для различных аккаунтов MegaMarket

5. Создайте файл `.env` в корневой директории проекта и добавьте необходимые переменные окружения.

## Использование

Запустите скрипт командой:

```
python main.py
```

## Структура проекта

```
project_root/
│
├── .env
├── main.py
├── README.md
├── requirements.txt
├── log.reader.py (функция для вывода всех логов в консоль)
│
├── scr/
│   ├── ascess/(Файлы аунтефикации для Гугл Таблиц)
│   ├── auth.py
│   ├── config.py
│   ├── data_fetcher.py
│   ├── data_writer.py
│   ├── logger.py
│   ├── parser_mm.py
│   ├── update_data_mm.py
│   └── update_mm.py
│
└── report/
    └── (generated CSV files)
```

## Логирование

Логи сохраняются в директории `logs/`. Проверяйте их для отслеживания процесса обновления и возможных ошибок.

## Отладка

Установите `DEBUG = True` в `main.py` для сохранения промежуточных результатов в CSV-файлах в директории `report/`,и тестовой отправки запросов к маркетплейсам (вывод в консоль).

## Вклад в проект

Пожалуйста, создавайте issues для сообщения о багах или предложения новых функций. Pull requests приветствуются.

## Лицензия

[MIT License](https://opensource.org/licenses/MIT)
