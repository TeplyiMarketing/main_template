from datetime import datetime, timedelta

from dotenv import set_key

from config import settings
from logs.logging import logger


def update_dates(range_dates: int):
    date_start = str(datetime.now().date() - timedelta(days=range_dates))
    date_now = str(datetime.now().date())
    settings.update_variable_env(['DATE_START', 'DATE_NOW'], [date_start, date_now])
    logger.info(f'Данные даты обновлены диапазон: С {date_start} по {date_now}!')
