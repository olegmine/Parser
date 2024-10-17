import asyncio
import aiohttp
import pandas as pd
import json
from .logger import logger


async def update_prices_mm(df, token, offer_id_col, price_col, is_deleted_col, debug=False):
    async with aiohttp.ClientSession() as session:
        url = "https://api.megamarket.tech/api/merchantIntegration/v1/offerService/manualPrice/save"

        prices = []
        for _, row in df.iterrows():
            offer_id = row[offer_id_col]
            isDeleted = False
            prices.append({
                "offerId": str(offer_id),
                "price": int(row[price_col]),
                "isDeleted": bool(isDeleted)
            })

        data = {
            "meta": {},
            "data": {
                "token": token,
                "prices": prices
            }
        }

        if debug:
            logger.warning("Отладочный режим для MM включен. Запрос не будет отправлен.")
            logger.warning("Отправляемые данные:")
            logger.info(json.dumps(data, indent=2))
        else:
            try:
                async with session.post(url, headers={"Content-Type": "application/json"},
                                        data=json.dumps(data)) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        logger.warning(f"Цены для товаров успешно обновлены!")
                        logger.warning(f"Ответ сервера: {response_data}")
                    else:
                        response_text = await response.text()
                        logger.error(f"Ошибка при отправке в МегаМаркет цен. Статус: {response.status}")
                        logger.error(f"Ответ сервера: {response_text}")
                        logger.warning(f"Заголовки ответа: {response.headers}")
            except aiohttp.ClientError as e:
                logger.error(f"Ошибка при отправке запроса: {str(e)}")
            except json.JSONDecodeError:
                logger.error("Ошибка при декодировании JSON-ответа")
            except Exception as e:
                logger.error(f"Неожиданная ошибка: {str(e)}")

            # Пример использования


if __name__ == "__main__":
    df = pd.DataFrame({
        "offer_id": ["103616"],
        "price": [2790],
        "is_deleted": [False]
    })

    token = "E20E64D6-DB3B-48C3-A22D-94963381F3F7"

    logger.info("Начало обновления цен в МегаМаркет")
    asyncio.run(update_prices_mm(df, token, "offer_id", "price", "is_deleted", debug=False))
    logger.info("Завершение обновления цен в МегаМаркет")