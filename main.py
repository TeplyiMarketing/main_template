import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine

from logs.logging import logger

from amocrm.amo_data import get_leads, merge_tables, get_events
from amocrm.columns import replace_dict_events, column_events, column_leads
from data.config import Config
from yandex.column_for_yandex import body, create_headers
from yandex.get_yandex import yandex, yandex_to_database

config = Config()
config.load_from_env('.env')
amocrm_data = config.get('amocrm')
yandex_data = config.get('yandex')
db = config.get('db')
start_amo_leads = datetime.strptime(amocrm_data.start_amo_leads, "%Y-%m-%d").timestamp()
finish_amo_leads = datetime.strptime(amocrm_data.finish_amo_leads, "%Y-%m-%d").timestamp()
start_amo_events = datetime.strptime(amocrm_data.start_amo_events, "%Y-%m-%d").timestamp()
finish_amo_events = datetime.strptime(amocrm_data.finish_amo_events, "%Y-%m-%d").timestamp()

link_leads = f'https://{amocrm_data.subdomain}.amocrm.ru/api/v4/leads?filter[created_at][from]={start_amo_leads}&filter[created_at][to]={finish_amo_leads}'
link_events = f'https://{amocrm_data.subdomain}.amocrm.ru/api/v4/events?filter[entity][]=lead&filter[type]=lead_status_changed&filter[created_at][from]={start_amo_events}&filter[created_at][to]={finish_amo_events}'
data_headers = {"Authorization": 'Bearer ' + amocrm_data.access_token}

link_postgres = f'postgresql://{db.user_db}:{db.password_db}@{db.address_db}:{db.port_db}/{db.name_db}'
engine = create_engine(link_postgres)


def run():
    df_leads = get_leads(link_leads, data_headers, column_leads)
    df_events = get_events(link_events, data_headers, replace_dict_events, column_events)
    if df_leads is not None and df_events is not None:
        merge_tables(engine, df_leads, df_events)
        logger.info("Объединение и загрузка данных прошла успешно!")
    else:
        # Обработка ситуации, если один из DataFrame равен None
        logger.warning("Ошибка при получении данных из API или чтении файлов.")

    # Yandex
    tokens = yandex_data.tokens
    logins = yandex_data.logins
    df_yandex = pd.DataFrame()
    for token, login in zip(tokens, logins):
        headers = create_headers(token, login)
        df1 = yandex(yandex_data.reports_url, body, headers)
        df_yandex = pd.concat([df_yandex, df1])
    try:
        df_yandex = df_yandex[df_yandex['Cost'] != 0]
        yandex_to_database(engine, df_yandex)
    except Exception as warning:
        logger.warning(f'Строки колонки Cost не были удалены. Код предупреждения - {warning}. Выгрузка не удалась!')


if __name__ == "__main__":
    run()
