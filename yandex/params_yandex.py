import json
import random

from data.config import Config

config = Config()
config.load_from_env('.env')
yandex_data = config.get('yandex')

body = {
    "params": {
        "SelectionCriteria": {"DateFrom": yandex_data.start_yandex_date, "DateTo": yandex_data.finish_yandex_date},
        "Goals": yandex_data.goals_id, "AttributionModels": ["LYDC"], "FieldNames": yandex_data.columns_yandex,
        "ReportName": f"Отчет {random.randint(1, 10000)}",
        "ReportType": "CRITERIA_PERFORMANCE_REPORT", "DateRangeType": "CUSTOM_DATE", "Format": "TSV",
        "IncludeVAT": "YES", "IncludeDiscount": "NO"}
}

body = json.dumps(body, indent=4)


def create_headers(token, login):
    return {
        'Authorization': f'Bearer ' + token,
        "Client-Login": login,
        "Accept-Language": "ru",
        "processingMode": "auto",
        "returnMoneyInMicros": "false",
        "skipReportHeader": "true",
        "skipReportSummary": "true",
    }
