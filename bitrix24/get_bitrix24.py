import pandas as pd

from amocrm.amo_data import split_utm_content
from config import settings
from data_to_db import create_data_in_db
from logs.logging import logger


def remove_duplicates(df, columns_bitrix24):
    columns_to_drop = [col for col in df.columns if col not in columns_bitrix24]

    removed_columns = []  # Список для хранения названий удаленных столбцов
    for i in columns_to_drop:
        try:
            df = df.drop(columns=i, axis=1)
            removed_columns.append(i)  # Добавление названия столбца в список
            return df
        except KeyError:
            logger.warning('Ключ не найден - пропускаем.Функция Bitrix24')
            return


def split_utm_campaign_and_insert(df, campaign_bitrix24):
    if campaign_bitrix24 not in df.columns:
        print(f"Столбец {campaign_bitrix24} не найден в DataFrame.")
        return df

    # Извлекаем ID из столбца 'UTM_campaign'
    df['UTM_CAMPAIGN_ID'] = df[campaign_bitrix24].str.split('-').str[-1]

    # Преобразуем 'UTM_campaign_id' в числовой формат
    df['UTM_CAMPAIGN_ID'] = pd.to_numeric(df['UTM_CAMPAIGN_ID'], errors='coerce')

    # Находим индекс столбца 'UTM_campaign'
    campaign_index = df.columns.get_loc(campaign_bitrix24) + 1

    # Создаем копию столбца 'UTM_campaign_id'
    campaign_id_column = df['UTM_CAMPAIGN_ID']

    # Удаляем 'UTM_campaign_id' из текущего положения
    df.drop('UTM_CAMPAIGN_ID', axis=1, inplace=True)

    # Вставляем 'UTM_campaign_id' рядом со столбцом 'UTM_campaign'
    df.insert(campaign_index, 'UTM_CAMPAIGN_ID', campaign_id_column)

    return df


def update_date_new_string(dataframe, list_merge_column: list, list_new_column: list):
    for first_column, second_column in zip(list_merge_column, list_new_column):
        dataframe[first_column] = dataframe[second_column].fillna(dataframe[first_column])


def get_deals_bitrix24(bitrix24, params, columns_bitrix24, replace_dict):
    try:
        deals = bitrix24.get_all('crm.deal.list', params)
        df_deals = pd.DataFrame(deals)
        strategy = bitrix24.get_all('crm.stagehistory.list', {'entityTypeId': 2, 'TYPE_ID': 2, })
        df_strategy = pd.DataFrame(strategy)

        merged_df = pd.merge(df_deals, df_strategy, left_on='ID', right_on='OWNER_ID',
                             how='left', validate="many_to_many")

        # Обновляем данные в новых строках
        update_date_new_string(dataframe=merged_df, list_merge_column=["MOVED_TIME", "STAGE_ID_x"],
                               list_new_column=['CREATED_TIME', 'STAGE_ID_y'])

        # Переименовываем столбцы
        merged_df.rename(columns={'ID_x': 'ID', 'STAGE_ID_x': 'STAGE_ID'}, inplace=True)
        if columns_bitrix24:
            remove_duplicates(merged_df, columns_bitrix24)

            if settings.column_utm_bitrix24 in columns_bitrix24:

                split_utm_content(merged_df)

                try:
                    df_deals = split_utm_content(merged_df)
                except Exception as error:
                    logger.warning(f'Ошибка разделения -  {error}.')

                if settings.column_campaign_bitrix24 in columns_bitrix24:
                    campaign_bitrix24 = settings.column_campaign_bitrix24

                    split_utm_campaign_and_insert(df_deals, campaign_bitrix24)

                    try:
                        df_deals = split_utm_campaign_and_insert(merged_df)
                    except Exception as error:
                        logger.warning(f'Ошибка разделения -  {error}.')

            if settings.removed_columns:
                logger.info(f'Удалены следующие столбцы: {", ".join(settings.removed_columns)}. Функция Bitrix24.')
            else:
                logger.info('Нет столбцов для удаления. Функция Bitrix24.')
        else:
            logger.warning('Отсутствует колонка.')
        merged_df['STAGE_ID'] = merged_df['STAGE_ID'].replace(replace_dict)
        merged_df['MOVED_TIME'] = pd.to_datetime(merged_df['MOVED_TIME']).dt.date
        create_data_in_db(merged_df, table_name='bitrix24', method="replace")
        logger.warning('Выгрузка таблицы Bitrix24 прошла успешно!')

    except Exception as error:
        logger.warning(f'Не удалось подключится к bitrix24 - {error}')
