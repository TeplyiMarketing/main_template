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
                    logger.warning('Ключ не найден - пропускаем.Функция Bitrix24')
            if 'UTM_CONTENT' in required_columns:
                def split_utm_content(df):
                    logger.debug("Начало обработки данных")

                    # Явно преобразуем все значения в столбце 'UTM_CONTENT' в строки
                    df['UTM_CONTENT'] = df['UTM_CONTENT'].apply(lambda x: '' if pd.isna(x) else str(x))
                    contains_groupid = df['UTM_CONTENT'].str.contains('groupid', na=False)

                    logger.debug(f"Строки, содержащие 'groupid': {df[contains_groupid].index.tolist()}")

                    if not contains_groupid.any():
                        logger.warning("Строк с 'groupid' не найдено. Пропуск разделения.")
                        return df

                    # Индекс для вставки новых столбцов сразу после 'UTM_CONTENT'
                    utm_index = df.columns.get_loc('UTM_CONTENT') + 1

                    # Перебираем строки DataFrame
                    for idx, content in df.iterrows():
                        if 'groupid' in content['UTM_CONTENT']:
                            delimiter = '||' if '||' in content['UTM_CONTENT'] else '//'
                            split_data = content['UTM_CONTENT'].split(delimiter)
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

                    logger.info("Разделение UTM_CONTENT выполнено")

                    return df

                try:
                    df_deals = split_utm_content(df_deals)
                except Exception as error:
                    logger.warning(f'Ошибка разделения -  {error}.')

                if 'UTM_CAMPAIGN' in required_columns:
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
