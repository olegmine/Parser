import pandas as pd
import numpy as np
import logging
import random
import asyncio
from concurrent.futures import ThreadPoolExecutor
from .logger import logger

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logger

# Создаем глобальный ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=4)


async def run_in_executor(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, func, *args)


async def update_dataframe(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Асинхронно обновляет первый DataFrame данными из второго DataFrame на основе seller_id.
    Обрабатывает случаи, когда типы данных seller_id могут отличаться.
    """

    def update_df():
        df1_updated = df1.copy()
        df2_updated = df2.copy()

        df1_updated['seller_id'] = df1_updated['seller_id'].astype(str)
        df2_updated['seller_id'] = df2_updated['seller_id'].astype(str)

        merged_df = df1_updated.merge(df2_updated[['seller_id', 'mp_on_market', 'market_with_mp']],
                                      on='seller_id',
                                      how='left',
                                      suffixes=('', '_new'))

        merged_df['mp_on_market'] = merged_df['mp_on_market_new'].fillna(merged_df['mp_on_market'])
        merged_df['market_with_mp'] = merged_df['market_with_mp_new'].fillna(merged_df['market_with_mp'])

        merged_df = merged_df.drop(['mp_on_market_new', 'market_with_mp_new'], axis=1)

        original_type = df1['seller_id'].dtype
        merged_df['seller_id'] = merged_df['seller_id'].astype(original_type)

        return merged_df

    return await run_in_executor(update_df)


async def compare_prices_and_create_for_update(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Асинхронно сравнивает цену из колонки price с mp_on_market и создает новый DataFrame for_update.
    Обновляет цену на целое число, которое на 50-200 рублей ниже mp_on_market, но не ниже stop.
    Логирует случаи, когда невозможно установить новую цену в заданных пределах.

    Параметры:
    df (pd.DataFrame): DataFrame с колонками seller_id, name, link, price, stop, mp_on_market, market_with_mp

    Возвращает:
    tuple: (pd.DataFrame, pd.DataFrame) - (Обновленный исходный DataFrame, Новый DataFrame for_update)
    """
    try:
        updated_df = df.copy()
        updated_df['prim'] = ''

        # Преобразуем числовые колонки
        numeric_columns = ['price', 'mp_on_market', 'stop']
        for col in numeric_columns:
            updated_df[col] = pd.to_numeric(updated_df[col], errors='coerce')

            # Проверяем на наличие NaN значений
        nan_mask = updated_df[numeric_columns].isna().any(axis=1)
        if nan_mask.any():
            logger.warning(f"Обнаружены NaN значения в {nan_mask.sum()} строках")
            logger.warning(updated_df[nan_mask].to_string())

            # Создаем маску для обновления
        mask = (updated_df['price'] > updated_df['mp_on_market']) & (updated_df['mp_on_market'] > updated_df['stop'])

        async def calculate_new_price(row):
            try:
                old_price = row['price']
                mp_on_market = row['mp_on_market']
                stop = row['stop']

                min_new_price = max(mp_on_market - 200, stop)
                max_new_price = mp_on_market - 50

                if min_new_price > max_new_price:
                    return old_price, f"Цена не изменена. Текущая цена: {old_price:.2f}, mp_on_market: {mp_on_market:.2f}, stop: {stop:.2f}"

                new_price = max(random.randint(int(min_new_price), int(max_new_price)), int(stop))
                return new_price, f"Цена изменена с {old_price:.2f} на {new_price:.2f} (mp_on_market: {mp_on_market:.2f})"
            except Exception as e:
                logger.error(f"Ошибка при расчете новой цены для товара {row['seller_id']}: {str(e)}")
                return row['price'], f"Ошибка при расчете новой цены: {str(e)}"

                # Асинхронно применяем функцию calculate_new_price

        results = await asyncio.gather(
            *[calculate_new_price(row) for _, row in updated_df[mask].iterrows()]
        )

        # Проверяем, есть ли результаты для обработки
        if results:
            new_prices, new_prims = zip(*results)
            updated_df.loc[mask, 'price'] = new_prices
            updated_df.loc[mask, 'prim'] = new_prims
        else:
            logger.info("Нет строк для обновления цен")

        for_update = updated_df[mask].copy()

        # Проверка на оптимальную цену
        for index, row in updated_df.iterrows():
            if row['mp_on_market'] <= row['stop']:
                warning_msg = f"Оптимальная цена mp_on_market ({row['mp_on_market']:.2f}) ниже или равна минимальной stop ({row['stop']:.2f}) для товара с артикулом {row['seller_id']}"
                logger.warning(warning_msg)
                updated_df.loc[index, 'prim'] = warning_msg

        return updated_df, for_update

    except Exception as e:
        logger.error(f"Критическая ошибка при обработке данных: {str(e)}")
        raise


async def main():
    # Пример использования:
    df1 = pd.DataFrame({
        'seller_id': [1, 2, 3, 4, 5],
        'name': ['Краска для волос', 'Bарвара', 'Cобака', 'Dом2', 'Eкспириэнс'],
        'link': ['link1', 'link2', 'link3', 'link4', 'link5'],
        'price': [1000, 200, 300, 400, 900],
        'stop': [150, 250, 350, 450, 550],
        'mp_on_market': [990, 20, 30, 40, 700],
        'market_with_mp': ['Gusi', 2, 3, 4, 'TopShop']
    })

    df2 = pd.DataFrame({
        'seller_id': ['1', '3', '6'],
        'name': ['A', 'C', 'F'],
        'mp_on_market': [800, 250, 60],
        'market_with_mp': ['Pizza', np.nan, 6]
    })

    # Обновляем df1 данными из df2
    updated_df = await update_dataframe(df1, df2)
    print("Обновленный DataFrame:")
    print(updated_df)
    print("\nТипы данных:")
    print(updated_df.dtypes)

    # Создаем DataFrame for_update и логируем случаи, когда mp_on_market ниже stop
    updated_df, for_update_df = await compare_prices_and_create_for_update(updated_df)
    print("\nDataFrame for_update:")
    print(for_update_df)
    print(updated_df)

    await run_in_executor(for_update_df.to_csv, 'report/reported2.txt')

    df = await run_in_executor(pd.read_csv, 'report/reported22.txt')
    print(df.info())


if __name__ == "__main__":
    asyncio.run(main())
