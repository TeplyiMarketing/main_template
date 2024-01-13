import pandas as pd
import requests as requests
from logs.logging import logger
import json


# Прописать текст всех ошибок
def amo_to_database(engine, merged_table):
    merged_table.to_sql(name='amocrm', con=engine, if_exists='replace', index=False)
    logger.info('Выгрузка с заменой. File amo_to_db.')


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
            logger.warning('Stop with ValueError.')
            break
        except KeyError:
            logger.warning('Key not found - pass. File leads.')
            break
        except Exception as error:
            logger.error(f'Возникла ошибка - {error}')
            pass
        logger.info(f'{pages_limit} - выполнено кругов цикла в leads')

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
            logger.error(f'Возникла ошибка - {error}')
            return pd.Series(dtype='object')

    # раскрыть столбец custom_fields_values в новые столбцы
    try:
        df = df.join(df.apply(expand_custom_fields, axis=1))
    except Exception as error:
        logger.warning(f'Возникли проблемы с custom_fields_values - {error}')

    if column_leads:
        columns_to_drop = [col for col in df.columns if col not in column_leads]
        for i in columns_to_drop:
            try:
                df = df.drop(columns=i, axis=1)
                logger.info(f'Удаление столбца {i} завершено успешно! Файл leads.')
            except KeyError:
                logger.warning('Ключ не найден - пропускаем. Файл leads.')
    else:
        logger.warning('Отсутствует columns_leads. Файл leads.')

    try:
        df['created_at'] = pd.to_datetime(df['created_at'], unit='s')
        df['created_at'] = df['created_at'].dt.date
        df['updated_at'] = pd.to_datetime(df['updated_at'], unit='s')
        df['updated_at'] = df['updated_at'].dt.date
    except Exception as error:
        logger.warning(f'Возникла ошибка to_datetime - {error}')
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
            logger.info('Stop with ValueError.')
            break
        except KeyError:
            logger.warning(f'Events were not received - KeyError')
            break
        except Exception as error:
            logger.error(f'Возникла ошибка - {error}')
            continue
        logger.info(f'{pages_limit} - выполнено кругов цикла в events')
    try:
        df_events['created_at'] = pd.to_datetime(df_events['created_at'], unit='s')
        df_events['created_at'] = df_events['created_at'].dt.date
    except KeyError:
        logger.warning(f'Events were not received - KeyError')
    except Exception as error:
        logger.error(f'Произошла непредвиденная ошибка - {error}')
    if column_events:
        columns_to_drop_events = [col for col in df_events.columns if col not in column_events]
        for i in columns_to_drop_events:
            try:
                df_events = df_events.drop(
                    columns=i, axis=1)
                logger.info(f'Drop column {i} finish with status - good! File events.')
            except KeyError:
                logger.warning('Key not found - pass. File events.')
            except Exception as error:
                logger.warning(f'Возникло исключение - {error}')
    try:
        df_events = df_events.rename(columns={'entity_id': 'id', 'created_at': 'date_event', 'value_after.id': 'stage'})
        df_events['stage'] = df_events['stage'].replace(replace_dict_events)
    except KeyError:
        logger.warning(f'Events were not received - KeyError')
    except Exception as error:
        logger.error(f'Произошла непредвиденная ошибка - {error}')
    return df_events


def merge_tables(engine, df1, df2):
    merged_table = pd.merge(df2, df1, on='id', how='left')
    merged_table = merged_table.dropna(subset=['name'])  # замените 'name' на нужный столбец
    try:
        merged_table = merged_table.drop(
            columns=['Unnamed: 0_x', 'Unnamed: 0_y'], axis=1)
        logger.info('Drop columns finish with status - good! File merge.')
    except KeyError:
        logger.warning('Key not found - pass. File merge.')
    # Сохранение объединенной таблицы в Excel файл
    try:
        merged_table['created_at'] = merged_table['created_at'].dt.date
        merged_table['date_event'] = merged_table['date_event'].dt.date
        merged_table['updated_at'] = merged_table['updated_at'].dt.date
    except Exception as error:
        logger.warning(f'Date not reversed {error}. File merge.')
    amo_to_database(engine, merged_table)
