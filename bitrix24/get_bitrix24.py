import pandas as pd
from logs.logging import logger


def get_deals(bitrix24, engine, params, columns_bitrix24, replace_dict):
    try:
        deals = bitrix24.get_all('crm.deal.list', params)
        df_deals = pd.DataFrame(deals)
        if columns_bitrix24:
            columns_to_drop = [col for col in df_deals.columns if col not in columns_bitrix24]
            removed_columns = []  # Список для хранения названий удаленных столбцов
            for i in columns_to_drop:
                try:
                    df_deals = df_deals.drop(columns=i, axis=1)
                    removed_columns.append(i)  # Добавление названия столбца в список
                except KeyError:
                    logger.warning('Ключ не найден - пропускаем.Функция Bitrix24')
            if 'UTM_CONTENT' in columns_bitrix24:
                def split_utm_content(df):
                    column_name = 'UTM_CONTENT'

                    logger.debug("Начало обработки данных")

                    # Явно преобразуем все значения в столбце 'UTM_CONTENT' в строки
                    df[column_name] = df[column_name].apply(lambda x: '' if pd.isna(x) else str(x))
                    contains_groupid = df[column_name].str.contains('groupid', na=False)

                    logger.debug(f"Строки, содержащие 'groupid': {df[contains_groupid].index.tolist()}")

                    if not contains_groupid.any():
                        logger.warning("Строк с 'groupid' не найдено. Пропуск разделения.")
                        return df

                    # Индекс для вставки новых столбцов сразу после 'UTM_CONTENT'
                    utm_index = df.columns.get_loc(column_name) + 1
                    groupid_found = False
                    # Перебираем строки DataFrame
                    for idx, content in df.iterrows():
                        if 'groupid' in content[column_name]:
                            groupid_found = True
                            delimiter = '||' if '||' in content[column_name] else '//'
                            split_data = content[column_name].split(delimiter)
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
                        logger.info(f"Обработка {column_name} завершена успешно.")
                    else:
                        logger.warning(f"Данные с 'groupid' не найдены в {column_name}.")

                    return df

                try:
                    df_deals = split_utm_content(df_deals)
                except Exception as error:
                    logger.warning(f'Ошибка разделения -  {error}.')

                if 'UTM_CAMPAIGN' in columns_bitrix24:
                    def split_utm_campaign_and_insert(df):
                        if 'UTM_CAMPAIGN' not in df.columns:
                            print("Столбец 'UTM_CAMPAIGN' не найден в DataFrame.")
                            return df

                        # Извлекаем ID из столбца 'UTM_campaign'
                        df['UTM_CAMPAIGN_ID'] = df['UTM_CAMPAIGN'].str.split('-').str[-1]

                        # Преобразуем 'UTM_campaign_id' в числовой формат
                        df['UTM_CAMPAIGN_ID'] = pd.to_numeric(df['UTM_CAMPAIGN_ID'], errors='coerce')

                        # Находим индекс столбца 'UTM_campaign'
                        campaign_index = df.columns.get_loc('UTM_CAMPAIGN') + 1

                        # Создаем копию столбца 'UTM_campaign_id'
                        campaign_id_column = df['UTM_CAMPAIGN_ID']

                        # Удаляем 'UTM_campaign_id' из текущего положения
                        df.drop('UTM_CAMPAIGN_ID', axis=1, inplace=True)

                        # Вставляем 'UTM_campaign_id' рядом со столбцом 'UTM_campaign'
                        df.insert(campaign_index, 'UTM_CAMPAIGN_ID', campaign_id_column)

                        return df

                    try:
                        df_deals = split_utm_campaign_and_insert(df_deals)
                    except Exception as error:
                        logger.warning(f'Ошибка разделения -  {error}.')

            if removed_columns:
                logger.info(f'Удалены следующие столбцы: {", ".join(removed_columns)}. Функция Bitrix24.')
            else:
                logger.info('Нет столбцов для удаления. Функция Bitrix24.')
        else:
            logger.warning('Отсутствует колонка.')
        # Проверьте, есть ли сделка уже в базе данных
        df_deals['STAGE_ID'] = df_deals['STAGE_ID'].replace(replace_dict)
        df_deals['MOVED_TIME'] = pd.to_datetime(df_deals['MOVED_TIME']).dt.date
        df_deals.to_sql(name='bitrix24', con=engine, if_exists='replace', index=False)
        logger.warning(f'Выгрузка таблицы Bitrix24 прошла успешно!')

    except Exception as error:
        logger.warning(f'Не удалось подключится к bitrix24 - {error}')
