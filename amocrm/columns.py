# id всегда обязательный!
column_leads = ['id', 'stage', 'name', 'created_at']

# entity_id and created_at and value_after.id обязательные!
column_events = ['entity_id', 'created_at', 'value_after.id']


replace_dict_events = {
    142: 'Успешно',
    143: 'Закрыто и не реализовано',
}
