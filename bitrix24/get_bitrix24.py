import pandas as pd
from logs.logging import logger


# def split_utm_content(df):
#     # Заменяем None на пустые строки
#     df['UTM_CONTENT'] = df['UTM_CONTENT'].fillna('')
#
#     # Разбиваем 'utm_content' на несколько столбцов по разделителю '//'
#     utm_df = df['UTM_CONTENT'].str.split('//', expand=True)
#
#     # Называем новые столбцы
#     utm_df.columns = [f'UTM_CONTENT_{col.split(":")[0]}' if ':' in col else col for col in utm_df.iloc[0]]
#
#     # Теперь разбиваем каждый из этих столбцов по ':', чтобы отделить ключи от значений
#     for col in utm_df.columns:
#         # Проверяем, содержит ли столбец символ ':'
#         if ':' in col:
#             utm_df[col] = utm_df[col].str.split(':').str[1]
#         else:
#             utm_df[col] = ''
#
#     # Добавляем новые столбцы обратно в исходный dataframe
#     df = pd.concat([df, utm_df], axis=1)
#
#     # Удаляем исходный столбец 'UTM_CONTENT'
#     df = df.drop('UTM_CONTENT', axis=1)
#
#     return df
#


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
                    logger.warning('Ключ не найден - пропускаем.Функция Bitrix24')
            if 'UTM_CONTENT' in required_columns:
                def split_utm_content(df):
                    # Находим первую непустую строку для определения названий колонок
                    first_non_empty = df['UTM_CONTENT'].dropna().iloc[0]
                    # Разбиваем её для создания списка названий колонок
                    column_names = [f'utm_content_{item.split(":")[0]}' for item in first_non_empty.split('//')]

                    # Разделяем столбец на несколько столбцов
                    utm_df = df['UTM_CONTENT'].str.split('//', expand=True)

                    # Обрезаем лишние столбцы если они есть
                    utm_df = utm_df.iloc[:, :len(column_names)]

                    # Присваиваем названия новым столбцам
                    utm_df.columns = column_names

                    # Разделяем значения в каждом из новых столбцов по ':'
                    for col in column_names:
                        utm_df[col] = utm_df[col].str.split(':').str[1]

                    # Добавляем новые столбцы обратно в исходный dataframe
                    df = pd.concat([df.drop('UTM_CONTENT', axis=1), utm_df], axis=1)

                    return df

                try:
                    # df_deals - ваш исходный DataFrame
                    df_deals = split_utm_content(df_deals)
                except Exception as error:
                    logger.warning(f'Отсутствует {error}.')

            if removed_columns:
                logger.info(f'Удалены следующие столбцы: {", ".join(removed_columns)}. Функция Bitrix24.')
            else:
                logger.info('Нет столбцов для удаления. Функция Bitrix24.')
        else:
            logger.warning('Отсутствует колонка.')
        # try:
        #     df_deals = split_utm_content(df_deals)
        # except Exception as error:
        #     logger.error(error)
        # Проверьте, есть ли сделка уже в базе данных
        df_deals['STAGE_ID'] = df_deals['STAGE_ID'].replace(replace_dict)
        df_deals['MOVED_TIME'] = pd.to_datetime(df_deals['MOVED_TIME']).dt.date
        df_deals.to_sql(name='bitrix24', con=engine, if_exists='replace', index=False)
        logger.warning(f'Выгрузка таблицы Bitrix24 прошла успешно!')

    except Exception as error:
        logger.warning(f'Не удалось подключится к bitrix24 - {error}')
