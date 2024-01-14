from datetime import datetime

dict_events = {
    142: 'Успешно',
    143: 'Закрыто и не реализовано',
}

dict_bitrix24 = {'EX_MPLE': "Пример"}  # Вставляем нужные нам этапы и как мы хотим их переименовать


def params_bitrix24(date_bitrix24, list_stages):
    date_bitrix24 = datetime.strptime(date_bitrix24, '%Y-%m-%d')
    params = {
        'filter': {
            '>DATE_CREATE': date_bitrix24.strftime('%Y-%m-%d'),
            'STAGE_ID': list_stages,
        }
    }

    return params
