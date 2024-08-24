import pandas as pd
from fast_bitrix24 import Bitrix
from sqlalchemy import create_engine

from amocrm.amo_data import get_leads, merge_tables, get_events
from amocrm.auth import authorization
from bitrix24.get_bitrix24 import get_deals
from config import settings
from data.dates import update_dates
from data.parameters import params_bitrix24, dict_bitrix24, dict_events
from logs.logging import logger
from yandex.get_yandex import yandex, yandex_to_database
from yandex.params_yandex import body, create_headers

engine = create_engine(settings.engine_connect_database)


# TODO сделать запуск обновления токенов 2 раза, а выгрузки 1
def run_update_tokens():
    assert authorization(env_path=settings.ENV_PATH, link_access=settings.LINK_ACCESS_AMOCRM,
                         response_data=settings.DATA_FOR_REQUEST_AMOCRM) == 200, \
        (logger.error("Ошибка в получении токена!"))


def run():
    update_dates(env_path=settings.env_path, range_dates=1)
    # AmoCRM
    logger.warning("Начался процесс выгрузки данных из AmoCRM системы!")
    df_leads = get_leads(settings.link_leads, settings.data_headers, settings.columns_leads)
    df_events = get_events(settings.link_events, settings.data_headers, dict_events, settings.columns_events)
    if df_leads is not None and df_events is not None:
        merge_tables(engine, df_leads, df_events)
    else:
        # Обработка ситуации, если один из DataFrame равен None
        logger.warning("Ошибка при получении данных из API или чтении файлов!")

    # Bitrix24
    logger.warning("Начался процесс выгрузки данных из Bitrix24 системы!")
    bitrix24 = Bitrix(settings.webhook_link)
    bitrix24_params = params_bitrix24(date_bitrix24=settings.date_now, list_stages=settings.stages)
    get_deals(bitrix24=bitrix24, engine=engine, params=bitrix24_params,
              columns_bitrix24=settings.columns_bitrix24, replace_dict=dict_bitrix24)

    # Yandex
    logger.warning("Начался процесс выгрузки данных из Yandex системы!")
    df_yandex = pd.DataFrame()
    for token, login in zip(settings.tokens, settings.logins):
        headers = create_headers(token, login)
        df1 = yandex(settings.reports_url, body, headers)
        df_yandex = pd.concat([df_yandex, df1])
    try:
        logger.warning("Начался процесс выгрузки данных из Yandex системы!")
        df_yandex = df_yandex[df_yandex['Cost'] != 0]
        yandex_to_database(engine, df_yandex)
        logger.info("Выгрузка данных Yandex прошла успешно!")
    except Exception as warning:
        logger.warning(f'Строки колонки Cost не были удалены. Код предупреждения - {warning}. Выгрузка не удалась!')


if __name__ == "__main__":
    run_update_tokens()
    run()
