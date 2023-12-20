# -*- coding: utf-8 -*-
# Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
import io
from time import sleep

import pandas as pd
import requests
from loguru import logger
from requests.exceptions import ConnectionError

from data.config_vars import Config


def yandex_to_database(engine, df1):
    try:
        df1.to_sql(name='yandex', con=engine, if_exists='append', index=False)
        logger.info("Выгрузка yandex прошла с добавлением. File yandex_to_db.")
    except:
        df1.to_sql(name='yandex', con=engine, if_exists='replace', index=False)
        logger.info("Выгрузка yandex прошла с заменой. File yandex_to_db.")


# --- Запуск цикла для выполнения запросов ---
# Если получен HTTP-код 200, то выводится содержание отчета
# Если получен HTTP-код 201 или 202, выполняются повторные запросы
def yandex(reports_url, body, headers):
    while True:
        try:
            config = Config()
            config.load_from_env('.env')
            request = requests.post(reports_url, body, headers=headers)
            if request.status_code == 400:
                logger.info("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                logger.info(f"JSON-код запроса: {body}")
                logger.info(f"JSON-код ответа сервера: \n{request.json()}")
                break
            elif request.status_code == 200:
                logger.info("Отчет создан успешно")
                return pd.read_csv(io.StringIO(request.text), sep='\t', encoding='utf-8', low_memory=False)
            elif request.status_code == 201:
                logger.info("Отчет успешно поставлен в очередь в режиме офлайн")
                retry_in = int(120)
                logger.info(f"Повторная отправка запроса через {retry_in} секунд")
                sleep(retry_in)
            elif request.status_code == 202:
                logger.info("Отчет успешно поставлен в очередь в режиме офлайн")
                retry_in = int(120)
                logger.info(f"Повторная отправка запроса через {retry_in} секунд")
                sleep(retry_in)
            elif request.status_code == 500:
                logger.info("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                logger.info(f"JSON-код ответа сервера: \n{request.json()}")
                break
            elif request.status_code == 502:
                logger.info("Время формирования отчета превысило серверное ограничение.")
                logger.info(
                    "Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                logger.info(f"JSON-код запроса: {body}")
                logger.info(f"JSON-код ответа сервера: \n{request.json()}")
                break
            else:
                logger.info("Произошла непредвиденная ошибка")
                logger.info(f"JSON-код запроса: {body}")
                logger.info(f"JSON-код ответа сервера: \n{request.json()}")
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            logger.error("Произошла ошибка соединения с сервером API")
            # Принудительный выход из цикла
            break

        except Exception as err:
            # В данном случае мы рекомендуем проанализировать действия приложения
            logger.error(f"Произошла непредвиденная ошибка {err}")
            # Принудительный выход из цикла
            break
