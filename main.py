import json

import pandas as pd
from fast_bitrix24 import Bitrix

from amocrm.amo_data import get_leads, merge_tables, get_events
from amocrm.auth import authorization
from bitrix24.get_bitrix24 import get_deals_bitrix24
from config import settings
from data.dates import update_dates
from data_to_db import create_data_in_db
from logs.logging import logger
from yandex.get_yandex import yandex


# TODO сделать запуск обновления токенов 2 раза, а выгрузки 1
def run_update_tokens():
    assert authorization(link_access=settings.LINK_ACCESS_AMOCRM,
                         response_data=settings.DATA_FOR_REQUEST_AMOCRM) == 200, \
        (logger.error("Ошибка в получении токена!"))


def run():
    update_dates(range_dates=1)
    # AmoCRM
    logger.warning("Начался процесс выгрузки данных из AmoCRM системы!")
    df_leads = get_leads(settings.link_leads, settings.data_headers, settings.columns_leads)
    df_events = get_events(settings.link_events, settings.data_headers, settings.dict_events, settings.columns_events)
    if df_leads is not None and df_events is not None:
        merge_tables(df_leads, df_events)
    else:
        # Обработка ситуации, если один из DataFrame равен None
        logger.warning("Ошибка при получении данных из API или чтении файлов!")

    # Bitrix24
    logger.warning("Начался процесс выгрузки данных из Bitrix24 системы!")
    bitrix24 = Bitrix(settings.webhook_link)
    bitrix24_params = settings.params_bitrix24()
    get_deals_bitrix24(bitrix24=bitrix24, params=bitrix24_params, columns_bitrix24=settings.columns_bitrix24,
                       replace_dict=dict_bitrix24)

    # Yandex
    logger.warning("Начался процесс выгрузки данных из Yandex системы!")
    df_yandex = pd.DataFrame()
    for token, login in zip(settings.tokens, settings.logins):
        headers = settings.create_headers(token, login)
        body = json.dumps(settings.yandex_params, indent=4)
        df1 = yandex(settings.reports_url, body, headers)
        df_yandex = pd.concat([df_yandex, df1])
    try:
        logger.warning("Начался процесс выгрузки данных из Yandex системы!")
        df_yandex = df_yandex[df_yandex['Cost'] != 0]
        create_data_in_db(df_yandex, table_name="yandex")
        logger.info("Выгрузка данных Yandex прошла успешно!")
    except Exception as warning:
        logger.warning(f'Строки колонки Cost не были удалены. Код предупреждения - {warning}. Выгрузка не удалась!')


if __name__ == "__main__":
    run_update_tokens()
    run()
