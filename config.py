import random
from datetime import datetime

from dotenv import set_key
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    mode: str | None
    env_path: str | None

    subdomain: str | None
    client_id: str | None
    client_secret: SecretStr | None
    redirect_uri: str | None
    refresh_token: SecretStr | None
    access_token: SecretStr | None
    columns_leads: str | None
    columns_events: str | None

    webhook_link: str | None
    columns_bitrix24: str | None
    stages: str | None

    reports_url: str | None
    logins: str | None
    tokens: str | None
    goals_id: str | None
    columns_yandex: str | None

    date_start: str | None
    date_now: str | None

    user_db: str | None
    password_db: str | None
    address_db: str | None
    port_db: int | None
    name_db: str | None

    column_utm_amocrm: str = 'UTM_content'
    column_campaign_amocrm: str = 'UTM_campaign'
    column_utm_bitrix24: str = 'UTM_CONTENT'
    column_campaign_bitrix24: str = 'UTM_CAMPAIGN'

    dict_bitrix24: dict = {
        'NEW': 'Заявка создана',
        'WON': "Успешно",
    }

    dict_events: dict = {
        142: 'Успешно',
        143: 'Закрыто и не реализовано',
    }

    @property
    def link_access_amocrm(self) -> str:
        return f'https://{self.subdomain}.amocrm.ru/oauth2/access_token'

    @property
    def data_headers(self) -> dict:
        return {"Authorization": f'Bearer {settings.access_token.get_secret_value()}'}

    @property
    def data_for_request_amocrm(self) -> dict:
        return {"client_id": self.client_id, "client_secret": self.client_secret.get_secret_value(),
                "grant_type": 'authorization_code',
                "code": self.refresh_token.get_secret_value(), "redirect_uri": self.redirect_uri}

    @property
    def link_leads(self) -> str:
        return (f'https://{self.subdomain}.amocrm.ru/api/v4/leads?filter[created_at][from]={self.date_start}'
                f'&filter[created_at][to]={self.date_now}')

    @property
    def link_events(self) -> str:
        return (f'https://{self.subdomain}.amocrm.ru/api/v4/events?filter[entity][]=lead&filter['
                f'type]=lead_status_changed&filter[created_at][from]={self.date_start}&filter[c'
                f'reated_at][to]={self.date_now}')

    @property
    def engine_connect_database(self) -> str:
        return (f'postgresql://{settings.user_db}:{settings.password_db}'
                f'@{settings.address_db}:{settings.port_db}/{settings.name_db}')

    @staticmethod
    def make_timestamp(date_format: str) -> int | float:
        return datetime.strptime(date_format, "%Y-%m-%d").timestamp()

    @property
    def yandex_params(self) -> dict:
        if self.goals_id != ['None']:
            self.yandex_params["params"]["Goals"] = settings.goals_id
            self.yandex_params["params"]["AttributionModels"] = ["LYDC"]
        return {
            "params": {
                "SelectionCriteria": {"DateFrom": self.date_start, "DateTo": self.date_now},
                "FieldNames": self.columns_yandex,
                "ReportName": f"Отчет {random.randint(1, 10000)}",
                "ReportType": "CRITERIA_PERFORMANCE_REPORT", "DateRangeType": "CUSTOM_DATE", "Format": "TSV",
                "IncludeVAT": "YES", "IncludeDiscount": "NO"}
        }

    @staticmethod
    def create_headers(token, login) -> dict:
        return {
            'Authorization': f'Bearer {token}',
            "Client-Login": login,
            "Accept-Language": "ru",
            "processingMode": "auto",
            "returnMoneyInMicros": "false",
            "skipReportHeader": "true",
            "skipReportSummary": "true",
        }

    def update_variable_env(self, variables: list, new_variables: list) -> None:
        for variable, new_variable in zip(variables, new_variables):
            set_key(self.env_path, variable, new_variable)

    def params_bitrix24(self) -> dict:
        date_bitrix24 = datetime.strptime(self.date_now, '%Y-%m-%d').strftime('%Y-%m-%d')
        params = {
            'filter': {
                '>DATE_CREATE': date_bitrix24,
                'STAGE_ID': self.stages,
            }
        }
        return params


settings = Settings()
