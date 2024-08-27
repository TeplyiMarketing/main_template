from loguru import logger
from sqlalchemy import create_engine

from config import settings


def create_data_in_db(dataframe, table_name: str, method: str = "append") -> None:
    """
    Функция необходимая для выгрузки полученных данных в базу данных
    :param dataframe: Данные которые нужно загрузить в базу.
    :param table_name: Название таблицы в базе данных.
    :param method: Метод выгрузки, с заменой или нет. Два метода (replace, append). По умолчанию - append.
    :return: None
    """
    engine = create_engine(settings.engine_connect_database)
    dataframe.to_sql(name=table_name, con=engine, if_exists=method, index=False)
    logger.info(f'Выгрузка {method}. Данные отправлены в Базу данных!')
