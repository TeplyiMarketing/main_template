import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine

from bitrix24.get_bitrix24 import get_deals
from data.replace_dicts import params_bitrix24, dict_bitrix24, dict_events
from logs.logging import logger
from fast_bitrix24 import Bitrix

from amocrm.amo_data import get_leads, merge_tables, get_events
from data.config import Config
from yandex.params_yandex import body, create_headers
from yandex.get_yandex import yandex, yandex_to_database, rename_conversions

config = Config()
config.load_from_env('.env')
amocrm_data = config.get('amocrm')
bitrix24_data = config.get('bitrix24')
yandex_data = config.get('yandex')
db = config.get('db')
start_amo_leads = datetime.strptime(amocrm_data.start_amo_leads, "%Y-%m-%d").timestamp()
finish_amo_leads = datetime.strptime(amocrm_data.finish_amo_leads, "%Y-%m-%d").timestamp()
start_amo_events = datetime.strptime(amocrm_data.start_amo_events, "%Y-%m-%d").timestamp()
finish_amo_events = datetime.strptime(amocrm_data.finish_amo_events, "%Y-%m-%d").timestamp()

link_leads = (f'https://{amocrm_data.subdomain}.amocrm.ru/api/v4/leads?filter[created_at][from]={start_amo_leads}'
              f'&filter[created_at][to]={finish_amo_leads}')
link_events = (f'https://{amocrm_data.subdomain}.amocrm.ru/api/v4/events?filter[entity][]=lead&filter['
               f'type]=lead_status_changed&filter[created_at][from]={start_amo_events}&filter[c'
               f'reated_at][to]={finish_amo_events}')
data_headers = {"Authorization": 'Bearer ' + amocrm_data.access_token}

link_postgres = f'postgresql://{db.user_db}:{db.password_db}@{db.address_db}:{db.port_db}/{db.name_db}'
engine = create_engine(link_postgres)


def run():
    # AmoCRM
    logger.warning("Начался процесс выгрузки данных из AmoCRM системы!")
    df_leads = get_leads(link_leads, data_headers, amocrm_data.columns_leads)
    df_events = get_events(link_events, data_headers, dict_events, amocrm_data.columns_events)
    if df_leads is not None and df_events is not None:
        merge_tables(engine, df_leads, df_events)
    else:
        # Обработка ситуации, если один из DataFrame равен None
        logger.warning("Ошибка при получении данных из API или чтении файлов!")

    # Bitrix24
    logger.warning("Начался процесс выгрузки данных из Bitrix24 системы!")
    bitrix24 = Bitrix(bitrix24_data.webhook_link)
    bitrix24_params = params_bitrix24(date_bitrix24=bitrix24_data.date_bitrix24, list_stages=bitrix24_data.stages)
    get_deals(bitrix24=bitrix24, engine=engine, params=bitrix24_params,
              required_columns=bitrix24_data.columns_bitrix24, replace_dict=dict_bitrix24)

    # Yandex
    logger.warning("Начался процесс выгрузки данных из Yandex системы!")
    tokens = yandex_data.tokens
    logins = yandex_data.logins
    df_yandex = pd.DataFrame()
    for token, login in zip(tokens, logins):
        headers = create_headers(token, login)
        df1 = yandex(yandex_data.reports_url, body, headers)
        df_yandex = pd.concat([df_yandex, df1])
        df_yandex = rename_conversions(df_yandex)
    try:
        logger.warning("Начался процесс выгрузки данных из Yandex системы!")
        df_yandex = df_yandex[df_yandex['Cost'] != 0]
        yandex_to_database(engine, df_yandex)
        logger.info("Выгрузка данных Yandex прошла успешно!")
    except Exception as warning:
        logger.warning(f'Строки колонки Cost не были удалены. Код предупреждения - {warning}. Выгрузка не удалась!')


if __name__ == "__main__":
    run()
