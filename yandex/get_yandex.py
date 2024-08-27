# -*- coding: utf-8 -*-
# Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
import io
import json
from time import sleep

import pandas as pd
import requests
from loguru import logger
from requests.exceptions import ConnectionError


# --- Запуск цикла для выполнения запросов ---
# Если получен HTTP-код 200, то выводится содержание отчета
# Если получен HTTP-код 201 или 202, выполняются повторные запросы
def yandex(reports_url: str, body: json, headers: dict):
    while True:
        try:
            request = requests.post(reports_url, body, headers=headers)
            if request.status_code == 400:
                logger.info("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди.\n"
                            f"JSON-код запроса: {body}.\n"
                            f"JSON-код ответа сервера: \n{request.json()}.")
                return 400
            elif request.status_code == 200:
                logger.warning("Отчёт yandex получен успешно, формирование таблицы. ")
                pd.read_csv(io.StringIO(request.text), sep='\t', encoding='utf-8', low_memory=False)
                return 200
            elif request.status_code == 201 or request.status_code == 202:
                logger.info("Отчет успешно поставлен в очередь в режиме офлайн.\n"
                            "Повторная отправка запроса через 60 секунд.")
                sleep(60)
            elif request.status_code == 500:
                logger.info(
                    "При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее.\n"
                    f"JSON-код ответа сервера: \n{request.json()}.")
                return 500
            elif request.status_code == 502:
                logger.info("Время формирования отчета превысило серверное ограничение.\n"
                            "Пожалуйста, попробуйте изменить параметры запроса - "
                            "уменьшить период и количество запрашиваемых данных.\n"
                            f"JSON-код запроса: {body}.\n"
                            f"JSON-код ответа сервера: \n{request.json()}.")
                return 500
            else:
                logger.info("Произошла непредвиденная ошибка.\n"
                            f"JSON-код запроса: {body}.\n"
                            f"JSON-код ответа сервера: \n{request.json()}.")
                break
        # Обработка ошибки, если не удалось соединиться с сервером API Директа
        except (ConnectionError, Exception) as error:
            # В данном случае мы рекомендуем повторить запрос позднее
            logger.error(f"Произошла ошибка {error}. Функция Yandex.")
            # Принудительный выход из цикла
            break
