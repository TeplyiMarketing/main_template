import json
import random

from data.config_vars import Config

config = Config()
config.load_from_env('.env')

body = {"params": {"SelectionCriteria": {"DateFrom": config.last_date_yandex, "DateTo": config.now_date_yandex},
                   "Goals": config.goals_id, "AttributionModels": ["LYDC"], "FieldNames": config.columns_yandex,
                   "ReportName": f"Отчет {random.randint(1, 10000)}",
                   "ReportType": "CRITERIA_PERFORMANCE_REPORT", "DateRangeType": "CUSTOM_DATE", "Format": "TSV",
                   "IncludeVAT": "YES", "IncludeDiscount": "NO"}
        }

body = json.dumps(body, indent=4)

headers = {'Authorization': f'{config.token_type} ' + config.token_yandex,
           "Client-Login": config.login_yandex,
           "Accept-Language": "ru",
           "processingMode": "auto",
           "returnMoneyInMicros": "false",
           "skipReportHeader": "true",
           "skipReportSummary": "true",
           }
