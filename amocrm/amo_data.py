import json

import pandas as pd
import requests as requests

from data.parameters import column_campaign_amocrm, column_utm_amocrm
from logs.logging import logger


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

    if column_utm_amocrm in column_leads:
        def split_utm_content(df):
            logger.debug("Начало обработки данных")

            # Явно преобразуем все значения в столбце 'UTM_CONTENT' в строки
            df[column_utm_amocrm] = df[column_utm_amocrm].apply(lambda x: '' if pd.isna(x) else str(x))
            contains_groupid = df[column_utm_amocrm].str.contains('groupid', na=False)

            logger.debug(f"Строки, содержащие 'groupid': {df[contains_groupid].index.tolist()}")

            if not contains_groupid.any():
                logger.warning("Строк с 'groupid' не найдено. Пропуск разделения.")
                return df

            # Индекс для вставки новых столбцов сразу после 'UTM_CONTENT'
            utm_index = df.columns.get_loc(column_utm_amocrm) + 1
            groupid_found = False
            # Перебираем строки DataFrame
            for idx, content in df.iterrows():
                if 'groupid' in content[column_utm_amocrm]:
                    groupid_found = True
                    delimiter = '||' if '||' in content[column_utm_amocrm] else '//'
                    split_data = content[column_utm_amocrm].split(delimiter)
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
                logger.info(f"Обработка {column_utm_amocrm} завершена успешно.")
            else:
                logger.warning(f"Данные с 'groupid' не найдены в {column_utm_amocrm}.")

            return df

        try:
            df = split_utm_content(df)
        except Exception as error:
            logger.warning(f'Ошибка разделения -  {error}.')

        if column_campaign_amocrm in column_leads:
            def split_utm_campaign_and_insert(df):
                if column_campaign_amocrm not in df.columns:
                    print(f"Столбец {column_campaign_amocrm} не найден в DataFrame.")
                    return df

                # Извлекаем ID из столбца 'UTM_campaign'
                df['UTM_campaign_id'] = df[column_campaign_amocrm].str.split('-').str[-1]

                # Преобразуем 'UTM_campaign_id' в числовой формат
                df['UTM_campaign_id'] = pd.to_numeric(df['UTM_campaign_id'], errors='coerce')

                # Находим индекс столбца 'UTM_campaign'
                campaign_index = df.columns.get_loc(column_campaign_amocrm) + 1

                # Создаем копию столбца 'UTM_campaign_id'
                campaign_id_column = df['UTM_campaign_id']

                # Удаляем 'UTM_campaign_id' из текущего положения
                df.drop('UTM_campaign_id', axis=1, inplace=True)

                # Вставляем 'UTM_campaign_id' рядом со столбцом 'UTM_campaign'
                df.insert(campaign_index, 'UTM_campaign_id', campaign_id_column)

                return df

            try:
                df = split_utm_campaign_and_insert(df)
            except Exception as error:
                logger.warning(f'Ошибка разделения -  {error}.')
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
        logger.error(f'Возникла ошибка при слиянии таблиц leads and events - {error}! '
                     f'Данные не были выгружены в базу данных!')
