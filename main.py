import asyncio
import random
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
from aiohttp import ClientError

from scr.config import (
    SAMPLE_SPREADSHEET_ID, UPDATE_INTERVAL_MINUTES,
    TECH_PC_COMPONENTS_MM, KLICK_MARKET_MM, BY_MARKET_MM,
    E_SHOPPER_MM, SSMART_SHOP_MM, ORIGINAL_MARKET_SHOP_MM,
    SQLITE_DB_NAME
)
from scr.data_fetcher import get_sheet_data, save_to_database
from scr.data_writer import write_sheet_data
from scr.logger import logger
from scr.parser_mm import scrape_megamarket
from scr.update_data_mm import compare_prices_and_create_for_update, update_dataframe
from scr.update_mm import update_prices_mm

# Глобальная переменная для режима отладки
DEBUG = True


async def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df.columns = ['seller_id', 'name', 'link', 'price', 'stop', 'mp_on_market', 'market_with_mp', 'prim']
        return df.iloc[1:]
    except Exception as e:
        logger.error(f"Ошибка при обработке DataFrame: {str(e)}")
        raise


async def save_debug_csv(df: pd.DataFrame, filename: str) -> None:
    if DEBUG:
        try:
            await asyncio.to_thread(df.to_csv, filename, index=False)
            logger.debug(f"Сохранен отладочный CSV: {filename}")
        except IOError as e:
            logger.error(f"Ошибка при сохранении отладочного CSV {filename}: {str(e)}")


async def process_megamarket_range(
        range_name: str,
        sheet_range: str,
        api_key: str,
        executor: ThreadPoolExecutor
) -> None:
    mm_logger = logger.bind(marketplace="MegaMarket", range=range_name)
    try:
        mm_logger.info("Начало обработки диапазона")

        df: Optional[pd.DataFrame] = None
        try:
            df = await get_sheet_data(SAMPLE_SPREADSHEET_ID, sheet_range)
            df = await process_dataframe(df)
        except Exception as e:
            mm_logger.error(f"Ошибка при получении данных из Google Sheets: {str(e)}")
            return

        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        await save_debug_csv(df, f"report/{range_name}{current_time}_first.csv")

        scraped_df: Optional[pd.DataFrame] = None
        try:
            scraped_df = await asyncio.get_event_loop().run_in_executor(executor, scrape_megamarket, df)
            await save_debug_csv(scraped_df, f"report/{range_name}{current_time}_scraped.csv")
        except Exception as e:
            mm_logger.error(f"Ошибка при скрапинге данных: {str(e)}")
            return

        try:
            updated_df = await update_dataframe(df, scraped_df)
            updated_df, for_update_df = await compare_prices_and_create_for_update(updated_df)
            await write_sheet_data(updated_df, SAMPLE_SPREADSHEET_ID, sheet_range.replace('1', '3'))
        except Exception as e:
            mm_logger.error(f"Ошибка при обновлении и сравнении данных: {str(e)}")
            return

        if not for_update_df.empty:
            mm_logger.info("Начало обновления цен через API", importance="high")
            try:
                await update_prices_mm(for_update_df, api_key, "seller_id", "price", "isDeleted", debug=DEBUG)
                mm_logger.info("Завершено обновление цен через API")
            except ClientError as e:
                mm_logger.error(f"Ошибка при обновлении цен через API: {str(e)}")
            except Exception as e:
                mm_logger.error(f"Неожиданная ошибка при обновлении цен: {str(e)}")

        await save_debug_csv(updated_df, f"report/{range_name}{current_time}_updated.csv")
        await save_debug_csv(for_update_df, f"report/{range_name}{current_time}_for_update.csv")

        mm_logger.info(f"Обработка диапазона {range_name} завершена")
    except Exception as e:
        mm_logger.error(f"Критическая ошибка при обработке диапазона{range_name}: {str(e)}", exc_info=True)


async def update_data_mm() -> None:
    mm_logger = logger.bind(marketplace="MegaMarket")
    try:
        mm_logger.warning("Начало обновления данных Mega Market")
        mm_ranges: List[Tuple[str, str, str]] = [
            ('ЮР1-Tech PC Components', 'MM_Tech_PC!A1:H', TECH_PC_COMPONENTS_MM),
            ('ЮР1-Klick-Market', 'MM_KlickMarket!A1:H', KLICK_MARKET_MM),
            ('ЮР2-ByMarket', 'MM_ByMarket!A1:H', BY_MARKET_MM),
            ('ЮР2-E-Shopper', 'MM_E-Shoper!A1:H', E_SHOPPER_MM),
            ('ЮР3-SSmart_shop', 'MM_SSmart_Shop!A1:H', SSMART_SHOP_MM),
            ('ЮР3-Original_Market', 'MM_Original_MS!A1:H', ORIGINAL_MARKET_SHOP_MM)
        ]

        with ThreadPoolExecutor() as executor:
            for i, (range_name, sheet_range, api_key) in enumerate(mm_ranges):
                if i > 0:
                    pause_duration = random.uniform(25 * 60, 40 * 60)
                    mm_logger.info(f"Пауза перед обработкой {range_name}: {pause_duration / 60:.2f} минут")
                    await asyncio.sleep(pause_duration)

                await process_megamarket_range(range_name, sheet_range, api_key, executor)

        mm_logger.info("Обновление данных Mega Market успешно завершено")
    except Exception as e:
        mm_logger.error(f"Критическая ошибка при обновлении данных Mega Market: {str(e)}", exc_info=True)


async def update_loop() -> None:
    while True:
        try:
            logger.info("Начало цикла обновления данных для ММ")
            await update_data_mm()
            logger.info("Цикл обновления данных для всех маркетплейсов успешно завершен")
        except Exception as e:
            logger.warning(f"Критическая ошибка в цикле обновления данных: {str(e)}")
        # logger.warning(f"Ожидание {UPDATE_INTERVAL_MINUTES} минут до следующего обновления")
        # await asyncio.sleep(UPDATE_INTERVAL_MINUTES * 60)


async def main() -> None:
    await update_loop()


if __name__ == "__main__":
    asyncio.run(main())


