import pandas as pd

from logs.logging import logger

from amocrm.amo_data import get_leads, merge_tables, get_events
from amocrm.columns import replace_dict_events, column_events, column_leads
from data.config_vars import Config
from yandex.column_for_yandex import body, create_headers
from yandex.get_yandex import yandex, yandex_to_database

config = Config()
config.load_from_env('.env')


def run():
    df_leads = get_leads(config.link_leads, config.data_headers, column_leads)
    df_events = get_events(config.link_events, config.data_headers, replace_dict_events, column_events)
    if df_leads is not None and df_events is not None:
        merge_tables(config.engine, df_leads, df_events)
        logger.info("Объединение и загрузка данных прошла успешно!")
    else:
        # Обработка ситуации, если один из DataFrame равен None
        logger.warning("Ошибка при получении данных из API или чтении файлов.")

    # Yandex
    tokens = config.tokens_yandex
    logins = config.logins_yandex
    df_yandex = pd.DataFrame()
    for token, login in zip(tokens, logins):
        headers = create_headers(token, login)
        df1 = yandex(config.reports_url, body, headers)
        df_yandex = pd.concat([df_yandex, df1])
    yandex_to_database(config.engine, df_yandex)


if __name__ == "__main__":
    run()
