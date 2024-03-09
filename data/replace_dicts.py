from datetime import datetime

dict_events = {
    142: 'Успешно',
    143: 'Закрыто и не реализовано',
}

# WON стандартный!
dict_bitrix24 = {
    'NEW': 'Заявка создана',
    'WON': "Успешно",
}

# Меняем на свои!
column_utm_amocrm = 'UTM_content'
column_utm_bitrix24 = 'UTM_CONTENT'


def params_bitrix24(date_bitrix24, list_stages):
    date_bitrix24 = datetime.strptime(date_bitrix24, '%Y-%m-%d')
    params = {
        'filter': {
            '>DATE_CREATE': date_bitrix24.strftime('%Y-%m-%d'),
            'STAGE_ID': list_stages,
        }
    }

    return params
