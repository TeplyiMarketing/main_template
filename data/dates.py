from datetime import datetime, timedelta

from dotenv import set_key

from logs.logging import logger


def update_dates(env_path: str, range_dates: int):
    date_start = str(datetime.now().date() - timedelta(days=range_dates))
    date_now = str(datetime.now().date())
    set_key(env_path, 'DATE_START', date_start)
    set_key(env_path, 'DATE_NOW', date_now)
    logger.info(f'Данные даты обновлены диапазон: С {date_start} по {date_now}!')
