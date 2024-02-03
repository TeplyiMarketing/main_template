import pandas as pd
import requests as requests
from logs.logging import logger
import json


# Прописать текст всех ошибок
def amo_to_database(engine, merged_table):
    merged_table.to_sql(name='amocrm', con=engine, if_exists='replace', index=False)
    logger.info('Выгрузка с заменой. Функция amo_to_db выполнена.')


def get_leads(link_leads, data_headers, column_leads=None):
    s = requests.Session()
    df = pd.DataFrame()
    pages_limit = 1
    while True:
        try:
            for i in range(pages_limit, 1000000):
                pages_limit += 1
                pages = {'page': i}
                leads = s.get(link_leads, headers=data_headers, params=pages)
                url_leads = leads.json()['_embedded']['leads']
                df_leads = pd.DataFrame(url_leads)
                df = pd.concat([df, df_leads], axis=0, ignore_index=True)
        except ValueError:
            logger.warning(
                f'Leads получены, остановка цикла - значений больше нет. Всего получено страниц - {pages_limit}.')
            break
        except KeyError:
            logger.warning('Ключ не был найден пропускаем. Функция leads.')
            break
        except Exception as error:
            logger.error(f'Возникла ошибка - {error}, функция leads.')
            pass

    # определить функцию для раскрытия столбца custom_fields_values
    def expand_custom_fields(row):
        try:
            values = row['custom_fields_values']
            if values is None:
                return pd.Series(dtype='object')  # Возвращаем пустой объект Series с именем столбца
            elif isinstance(values, str):
                values = json.loads(values)
            else:
                pass
            result = {}
            for value in values:
                result[value['field_name']] = value['values'][0]['value']
            return pd.Series(result)
        except Exception as error:
            logger.error(f'Возникла ошибка - {error}, функция leads.')
            return pd.Series(dtype='object')

    # раскрыть столбец custom_fields_values в новые столбцы
    try:
        df = df.join(df.apply(expand_custom_fields, axis=1))
    except Exception as error:
        logger.warning(f'Возникли проблемы с custom_fields_values - {error}.')

    if column_leads:
        columns_to_drop = [col for col in df.columns if col not in column_leads]
        removed_columns = []  # Список для хранения названий удаленных столбцов
        for i in columns_to_drop:
            try:
                df = df.drop(columns=i, axis=1)
                removed_columns.append(i)  # Добавление названия столбца в список
            except KeyError:
                logger.warning('Ключ не найден - пропускаем. Функция leads.')
        if removed_columns:
            logger.info(f'Удалены следующие столбцы: {", ".join(removed_columns)}. Функция leads.')
        else:
            logger.info('Нет столбцов для удаления. Функция leads.')
    else:
        logger.warning('Отсутствует columns_leads. Функция leads.')

    if 'utm_content' in column_leads:
        def split_utm_content(df):
            # Разбиваем 'utm_content' на несколько столбцов по разделителю '||'
            utm_df = df['utm_content'].str.split('\|\|', expand=True)

            # Называем новые столбцы
            utm_df.columns = [f'utm_content_{col.split(":")[0]}' for col in utm_df.iloc[0]]

            # Теперь разбиваем каждый из этих столбцов по ':', чтобы отделить ключи от значений
            for col in utm_df.columns:
                utm_df[col] = utm_df[col].str.split(':').str[1]

            # Добавляем новые столбцы обратно в исходный dataframe
            df = pd.concat([df, utm_df], axis=1)

            # Удаляем исходный столбец 'utm_content'
            df = df.drop('utm_content', axis=1)

            return df

        df = split_utm_content(df)
    else:
        logger.info('Нет столбцов для удаления. Функция leads.')

    try:
        df['created_at'] = pd.to_datetime(df['created_at'], unit='s')
        df['created_at'] = df['created_at'].dt.date
        df['updated_at'] = pd.to_datetime(df['updated_at'], unit='s')
        df['updated_at'] = df['updated_at'].dt.date
    except Exception as error:
        logger.warning(f'Возникла ошибка to_datetime - {error}, в функции leads.')
    return df


def get_events(link_events, data_headers, replace_dict_events, column_events=None):
    s = requests.Session()
    df_events = pd.DataFrame()
    pages_limit = 1
    while True:
        try:
            for i in range(pages_limit, 1000000):
                pages_limit += 1
                pages = {'page': i}
                events = s.get(link_events, headers=data_headers, params=pages)
                url_events = events.json()['_embedded']['events']
                df_events = pd.concat([df_events, pd.DataFrame(url_events)], axis=0, ignore_index=True)
                df_events['value_after.id'] = df_events['value_after'].apply(lambda x: x[0]['lead_status']['id'])
        except ValueError:
            logger.warning(
                f'Events получены, остановка цикла - значений больше нет. Всего получено страниц - {pages_limit}.')
            break
        except KeyError:
            logger.warning(f'Ключ не найден - пропускаем. Функция Events.')
            break
        except Exception as error:
            logger.error(f'Возникла ошибка - {error}, функция events.')
            continue
    try:
        df_events['created_at'] = pd.to_datetime(df_events['created_at'], unit='s')
        df_events['created_at'] = df_events['created_at'].dt.date
    except KeyError:
        logger.warning(f'Ключ не найден - пропускаем. Функция Events.')
    except Exception as error:
        logger.error(f'Произошла непредвиденная ошибка - {error}, функция events.')
    if column_events:
        columns_to_drop_events = [col for col in df_events.columns if col not in column_events]
        removed_columns = []  # Список для хранения названий удаленных столбцов
        for i in columns_to_drop_events:
            try:
                df_events = df_events.drop(
                    columns=i, axis=1)
                removed_columns.append(i)  # Добавление названия столбца в список
            except KeyError:
                logger.warning(f'Ключ не найден - пропускаем. Функция Events.')
            except Exception as error:
                logger.warning(f'Возникло исключение - {error}, функция events.')
        if removed_columns:
            logger.info(f'Удалены следующие столбцы: {", ".join(removed_columns)}. Функция events.')
        else:
            logger.info('Нет столбцов для удаления. Функция events.')
    else:
        logger.warning('Отсутствует columns_events. Функция events.')
    try:
        df_events = df_events.rename(columns={'entity_id': 'id', 'created_at': 'date_event', 'value_after.id': 'stage'})
        df_events['stage'] = df_events['stage'].replace(replace_dict_events)
    except KeyError:
        logger.warning(f'Ключ не найден - пропускаем. Функция Events.')
    except Exception as error:
        logger.error(f'Произошла непредвиденная ошибка - {error}, функция events.')
    return df_events


def merge_tables(engine, df1, df2):
    try:
        merged_table = pd.merge(df2, df1, on='id', how='left')
        merged_table = merged_table.dropna(subset=['name'])  # замените 'name' на нужный столбец
        try:
            merged_table = merged_table.drop(
                columns=['Unnamed: 0_x', 'Unnamed: 0_y'], axis=1)
            logger.info('Удаление колонок прошло успешно! Функция объединения - merge_tables.')
        except KeyError:
            logger.warning(f'Ключ не найден - пропускаем. Функция Merge_tables.')
        # Сохранение объединенной таблицы в Excel файл
        try:
            merged_table['created_at'] = merged_table['created_at'].dt.date
            merged_table['date_event'] = merged_table['date_event'].dt.date
            merged_table['updated_at'] = merged_table['updated_at'].dt.date
        except Exception as error:
            logger.warning(f'Дата не была поменяна - {error}. Функция объединения - merge_tables.')
        amo_to_database(engine, merged_table)
    except Exception as error:
        logger.error(f'Возникла ошибка при слиянии таблиц leads and events - {error}!'
                     f'Данные не были выгружены в базу данных!')
