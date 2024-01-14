import pandas as pd
from logs.logging import logger


def get_deals(bitrix24, engine, params, required_columns, replace_dict):
    try:
        deals = bitrix24.get_all('crm.deal.list', params)
        df_deals = pd.DataFrame(deals)
        if required_columns:
            columns_to_drop = [col for col in df_deals.columns if col not in required_columns]
            removed_columns = []  # Список для хранения названий удаленных столбцов
            for i in columns_to_drop:
                try:
                    df_deals = df_deals.drop(columns=i, axis=1)
                    removed_columns.append(i)  # Добавление названия столбца в список
                except KeyError:
                    logger.warning('Ключ не найден - пропускаем.')
            if removed_columns:
                logger.info(f'Удалены следующие столбцы: {", ".join(removed_columns)}. Функция Bitrix24.')
            else:
                logger.info('Нет столбцов для удаления. Функция events.')
        else:
            logger.warning('Отсутствует колонка.')

        # Проверьте, есть ли сделка уже в базе данных
        df_deals['STAGE_ID'] = df_deals['STAGE_ID'].replace(replace_dict)
        df_deals['MOVED_TIME'] = pd.to_datetime(df_deals['MOVED_TIME']).dt.date
        df_deals.to_sql(name='bitrix24', con=engine, if_exists='replace', index=False)
        logger.warning(f'Выгрузка таблицы Bitrix24 прошла успешно!')

    except Exception as error:
        logger.warning(f'Не удалось подключится к bitrix24 - {error}')
