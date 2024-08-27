import json

import pandas as pd
import requests as requests

from config import settings
from data_to_db import create_data_in_db
from logs.logging import logger


def get_leads_data(session, link_leads, data_headers):
    pages_limit = 1
    dataframe_leads = pd.DataFrame()
    while True:
        try:
            for i in range(pages_limit, 1000000):
                pages_limit += 1
                pages = {'page': i}
                leads = session.get(link_leads, headers=data_headers, params=pages)
                url_leads = leads.json()['_embedded']['leads']
                df_leads = pd.DataFrame(url_leads)
                dataframe_leads = pd.concat([dataframe_leads, df_leads], axis=0, ignore_index=True)
        except ValueError:
            logger.warning(
                f'Leads получены, остановка цикла - значений больше нет. Всего получено страниц - {pages_limit}.')
            return dataframe_leads
        except KeyError:
            logger.warning('Ключ не был найден пропускаем. Функция leads.')
        except Exception as error:
            logger.error(f'Возникла ошибка - {error}, функция leads.')


def expand_custom_fields(row):
    try:
        values = row['custom_fields_values']
        if values is None:
            return pd.Series(dtype='object')
        elif isinstance(values, str):
            values = json.loads(values)
        result = {}
        for value in values:
            result[value['field_name']] = value['values'][0]['value']
        return pd.Series(result)
    except Exception as error:
        logger.error(f'Возникла ошибка - {error}, функция leads.')
        return pd.Series(dtype='object')


def split_utm_content(df):
    logger.debug("Начало обработки данных")

    # Явно преобразуем все значения в столбце 'UTM_CONTENT' в строки
    df[settings.column_utm_amocrm] = df[settings.column_utm_amocrm].apply(lambda x: '' if pd.isna(x) else str(x))
    contains_groupid = df[settings.column_utm_amocrm].str.contains('groupid', na=False)

    logger.debug(f"Строки, содержащие 'groupid': {df[contains_groupid].index.tolist()}")

    if not contains_groupid.any():
        logger.warning("Строк с 'groupid' не найдено. Пропуск разделения.")
        return df

    # Индекс для вставки новых столбцов сразу после 'UTM_CONTENT'
    utm_index = df.columns.get_loc(settings.column_utm_amocrm) + 1
    groupid_found = False
    # Перебираем строки DataFrame
    for idx, content in df.iterrows():
        if 'groupid' in content[settings.column_utm_amocrm]:
            groupid_found = True
            delimiter = '||' if '||' in content[settings.column_utm_amocrm] else '//'
            split_data = content[settings.column_utm_amocrm].split(delimiter)
            split_dict = {}
            for item in split_data:
                if ':' in item:
                    key, value = item.split(':', 1)
                    split_dict[f'utm_content_{key.strip()}'] = value.strip()
            # Для каждого ключа в split_dict добавляем значение в соответствующий столбец DataFrame
            for key, value in split_dict.items():
                # Если столбец еще не существует, создаем его
                if key not in df:
                    df.insert(utm_index, key, None)
                    utm_index += 1
                df.at[idx, key] = value

    if groupid_found:
        logger.info(f"Обработка {settings.column_utm_amocrm} завершена успешно.")
    else:
        logger.warning(f"Данные с 'groupid' не найдены в {settings.column_utm_amocrm}.")
    return df


def split_utm_campaign_and_insert(df):
    if settings.column_campaign_amocrm not in df.columns:
        print(f"Столбец {settings.column_campaign_amocrm} не найден в DataFrame.")
        return df

    # Извлекаем ID из столбца 'UTM_campaign'
    df['UTM_campaign_id'] = df[settings.column_campaign_amocrm].str.split('-').str[-1]

    # Преобразуем 'UTM_campaign_id' в числовой формат
    df['UTM_campaign_id'] = pd.to_numeric(df['UTM_campaign_id'], errors='coerce')

    # Находим индекс столбца 'UTM_campaign'
    campaign_index = df.columns.get_loc(settings.column_campaign_amocrm) + 1

    # Создаем копию столбца 'UTM_campaign_id'
    campaign_id_column = df['UTM_campaign_id']

    # Удаляем 'UTM_campaign_id' из текущего положения
    df.drop('UTM_campaign_id', axis=1, inplace=True)

    # Вставляем 'UTM_campaign_id' рядом со столбцом 'UTM_campaign'
    df.insert(campaign_index, 'UTM_campaign_id', campaign_id_column)

    return df


def replace_data_to_date(dataframe, variables: list):
    for variable in variables:
        try:
            dataframe[variable].dt.date
        except Exception as error:
            logger.warning(f'Дата не была поменяна - {error}. Функция объединения - merge_tables.')


def get_leads(link_leads, data_headers, column_leads=None):
    session = requests.Session()
    try:
        dataframe_leads = get_leads_data(session=session, link_leads=link_leads, data_headers=data_headers)
        dataframe_leads = dataframe_leads.join(dataframe_leads.apply(expand_custom_fields, axis=1))
    except Exception as error:
        logger.warning(f'Возникли проблемы с custom_fields_values - {error}.')

    if column_leads:
        columns_to_drop = [col for col in dataframe_leads.columns if col not in column_leads]
        removed_columns = []  # Список для хранения названий удаленных столбцов
        for i in columns_to_drop:
            try:
                dataframe_leads = dataframe_leads.drop(columns=i, axis=1)
                removed_columns.append(i)  # Добавление названия столбца в список
            except KeyError:
                logger.warning('Ключ не найден - пропускаем. Функция leads.')
        if removed_columns:
            logger.info(f'Удалены следующие столбцы: {", ".join(removed_columns)}. Функция leads.')
        else:
            logger.info('Нет столбцов для удаления. Функция leads.')
    else:
        logger.warning('Отсутствует columns_leads. Функция leads.')

    if settings.column_utm_amocrm in column_leads:
        try:
            dataframe_leads = split_utm_content(dataframe_leads)
        except Exception as error:
            logger.warning(f'Ошибка разделения -  {error}.')

        if settings.column_campaign_amocrm in column_leads:

            try:
                dataframe_leads = split_utm_campaign_and_insert(dataframe_leads)
            except Exception as error:
                logger.warning(f'Ошибка разделения -  {error}.')
    else:
        logger.info('Нет столбцов для удаления. Функция leads.')

    try:
        dataframe_leads['created_at'] = pd.to_datetime(dataframe_leads['created_at'], unit='s')
        dataframe_leads['updated_at'] = pd.to_datetime(dataframe_leads['updated_at'], unit='s')
        replace_data_to_date(dataframe_leads, ["created_at", "updated_at"])
    except Exception as error:
        logger.warning(f'Возникла ошибка to_datetime - {error}, в функции leads.')
    return dataframe_leads


def drops_events_columns(dataframe):
    columns_to_drop_events = [col for col in dataframe.columns if col not in settings.column_events]
    removed_columns = []  # Список для хранения названий удаленных столбцов
    for i in columns_to_drop_events:
        try:
            dataframe = dataframe.drop(
                columns=i, axis=1)
            removed_columns.append(i)  # Добавление названия столбца в список
            return dataframe
        except KeyError:
            logger.warning('Ключ не найден - пропускаем. Функция Events.')
        except Exception as error:
            logger.warning(f'Возникло исключение - {error}, функция events.')
    if removed_columns:
        logger.info(f'Удалены следующие столбцы: {", ".join(removed_columns)}. Функция events.')
    else:
        logger.info('Нет столбцов для удаления. Функция events.')


def get_events_columns(session):
    df_events = pd.DataFrame()
    pages_limit = 1
    try:
        for i in range(pages_limit, 1000000):
            pages_limit += 1
            pages = {'page': i}
            events = session.get(settings.link_events, headers=settings.data_headers, params=pages)
            url_events = events.json()['_embedded']['events']
            df_events = pd.concat([df_events, pd.DataFrame(url_events)], axis=0, ignore_index=True)
            df_events['value_after.id'] = df_events['value_after'].apply(lambda x: x[0]['lead_status']['id'])
    except ValueError:
        logger.warning(
            f'Events получены, остановка цикла - значений больше нет. Всего получено страниц - {pages_limit}.')
        return df_events
    except KeyError:
        logger.warning('Ключ не найден - пропускаем. Функция Events.')
    except Exception as error:
        logger.error(f'Возникла ошибка - {error}, функция events.')


def get_events(column_events=None):
    session = requests.Session()
    message_error = 'Ключ не найден - пропускаем. Функция Events.'
    df_events = get_events_columns(session)
    try:
        df_events['created_at'] = pd.to_datetime(df_events['created_at'], unit='s')
        df_events['created_at'] = df_events['created_at'].dt.date
    except KeyError:
        logger.warning(message_error)
    except Exception as error:
        logger.error(f'Произошла непредвиденная ошибка - {error}, функция events.')
    if column_events:
        drops_events_columns(df_events)
    else:
        logger.warning('Отсутствует columns_events. Функция events.')
    try:
        df_events = df_events.rename(columns={'entity_id': 'id', 'created_at': 'date_event', 'value_after.id': 'stage'})
        df_events['stage'] = df_events['stage'].replace(settings.replace_dict_events)
    except KeyError:
        logger.warning(message_error)
    except Exception as error:
        logger.error(f'Произошла непредвиденная ошибка - {error}, функция events.')
    return df_events


def merge_tables(dataframe_first, dataframe_second):
    try:
        merged_table = pd.merge(dataframe_second, dataframe_first, on='id', how='left', validate="many_to_many")
        merged_table = merged_table.dropna(subset=['name'])  # замените 'name' на нужный столбец
        try:
            merged_table = merged_table.drop(
                columns=['Unnamed: 0_x', 'Unnamed: 0_y'], axis=1)
            logger.info('Удаление колонок прошло успешно! Функция объединения - merge_tables.')
        except KeyError:
            logger.warning('Ключ не найден - пропускаем. Функция Merge_tables.')
        # Сохранение объединенной таблицы в Excel файл
        replace_data_to_date(merged_table, ['created_at', 'date_event', 'updated_at'])

        create_data_in_db(merged_table, "amocrm")
    except Exception as error:
        logger.error(f'Возникла ошибка при слиянии таблиц leads and events - {error}! '
                     f'Данные не были выгружены в базу данных!')
