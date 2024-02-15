# -*- coding: utf-8 -*-
# Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
import io
from time import sleep

import pandas as pd
import requests
from loguru import logger
from requests.exceptions import ConnectionError


def rename_conversions(df):
    # Найдите все колонки, которые содержат 'Conversion' в их названии
    conversion_columns = [col for col in df.columns if 'Conversions' in col]

    # Создайте словарь с новыми именами
    rename_dict = {}
    for i, col in enumerate(conversion_columns, start=1):
        if i == 1:
            rename_dict[col] = 'Conversions'
        else:
            rename_dict[col] = f'Conversions_{i}'

    # Переименуйте колонки в DataFrame
    df = df.rename(columns=rename_dict)

    return df


def yandex_to_database(engine, df1):
    try:
        df1.to_sql(name='yandex', con=engine, if_exists='append', index=False)
        logger.warning("Выгрузка yandex прошла с добавлением. Функция yandex_to_db.")
    except Exception as err:
        df1.to_sql(name='yandex', con=engine, if_exists='replace', index=False)
        logger.warning(f"Выгрузка yandex прошла с заменой. Функция yandex_to_db. {err}")


# --- Запуск цикла для выполнения запросов ---
# Если получен HTTP-код 200, то выводится содержание отчета
# Если получен HTTP-код 201 или 202, выполняются повторные запросы
def yandex(reports_url, body, headers):
    while True:
        retry_in = int(120)
        try:
            request = requests.post(reports_url, body, headers=headers)
            if request.status_code == 400:
                logger.info("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди.")
                logger.info(f"JSON-код запроса: {body}.")
                logger.info(f"JSON-код ответа сервера: \n{request.json()}.")
                break
            elif request.status_code == 200:
                logger.warning("Отчёт yandex получен успешно, формирование таблицы. ")
                return pd.read_csv(io.StringIO(request.text), sep='\t', encoding='utf-8', low_memory=False)
            elif request.status_code == 201:
                logger.info("Отчет успешно поставлен в очередь в режиме офлайн.")
                logger.info(f"Повторная отправка запроса через {retry_in} секунд.")
                sleep(retry_in)
            elif request.status_code == 202:
                logger.info("Отчет успешно поставлен в очередь в режиме офлайн.")
                logger.info(f"Повторная отправка запроса через {retry_in} секунд.")
                sleep(retry_in)
            elif request.status_code == 500:
                logger.info(
                    "При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее.")
                logger.info(f"JSON-код ответа сервера: \n{request.json()}.")
                break
            elif request.status_code == 502:
                logger.info("Время формирования отчета превысило серверное ограничение.")
                logger.info(
                    "Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                logger.info(f"JSON-код запроса: {body}.")
                logger.info(f"JSON-код ответа сервера: \n{request.json()}.")
                break
            else:
                logger.info("Произошла непредвиденная ошибка.")
                logger.info(f"JSON-код запроса: {body}.")
                logger.info(f"JSON-код ответа сервера: \n{request.json()}.")
                break

        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except ConnectionError:
            # В данном случае мы рекомендуем повторить запрос позднее
            logger.error("Произошла ошибка соединения с сервером API. Функция Yandex.")
            # Принудительный выход из цикла
            break

        except Exception as err:
            # В данном случае мы рекомендуем проанализировать действия приложения
            logger.error(f"Произошла непредвиденная ошибка {err}. Функция Yandex.")
            # Принудительный выход из цикла
            break
