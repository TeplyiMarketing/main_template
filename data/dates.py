from datetime import datetime, timedelta
from logs.logging import logger
from dotenv import set_key


def update_dates_amocrm():
    now_date_amo = datetime.now().date()
    formatted_now = now_date_amo.strftime('%Y-%m-%d')
    set_key('.env', 'NOW_DATE_AMO', str(formatted_now))
    logger.info(f'Update date Amo - {formatted_now}')


def update_dates_yandex():
    now_date_yandex = datetime.now().date()
    last_date_yandex = now_date_yandex - timedelta(days=1)
    # Преобразовать дату в строку в нужном формате (YYYY-MM-DD)
    formatted_last = last_date_yandex.strftime('%Y-%m-%d')
    set_key('.env', 'NOW_DATE_YANDEX', str(formatted_last))
    logger.info(f'Update date Yandex - {formatted_last}')
