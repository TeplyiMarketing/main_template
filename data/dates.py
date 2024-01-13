from datetime import datetime, timedelta
from logs.logging import logger
from dotenv import set_key


def update_dates_amocrm():
    finish_date_leads = datetime.now().date()
    finish_date_events = datetime.now().date()
    formatted_finish_leads = finish_date_leads.strftime('%Y-%m-%d')
    formatted_finish_events = finish_date_events.strftime('%Y-%m-%d')
    set_key('.env', 'FINISH_AMO_LEADS', str(formatted_finish_leads))
    set_key('.env', 'FINISH_AMO_EVENTS', str(formatted_finish_events))
    logger.info(f'Update date leads and events - {formatted_finish_leads, formatted_finish_events}')


def update_dates_yandex():
    finish_date_yandex = datetime.now().date()
    start_date_yandex = finish_date_yandex - timedelta(days=1)
    # Преобразовать дату в строку в нужном формате (YYYY-MM-DD)
    formatted_start = start_date_yandex.strftime('%Y-%m-%d')
    set_key('.env', 'FINISH_YANDEX_DATE', str(finish_date_yandex))
    set_key('.env', 'START_YANDEX_DATE', str(formatted_start))
    logger.info(f'Update date Yandex - {formatted_start}')
