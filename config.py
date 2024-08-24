from datetime import datetime
from typing import Optional

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    mode: Optional[str]
    env_path: Optional[str]

    subdomain: Optional[str]
    client_id: Optional[str]
    client_secret: Optional[SecretStr]
    redirect_uri: Optional[str]
    refresh_token: Optional[SecretStr]
    access_token: Optional[SecretStr]
    columns_leads: Optional[str]
    columns_events: Optional[str]

    webhook_link: Optional[str]
    columns_bitrix24: Optional[str]
    stages: Optional[str]

    reports_url: Optional[str]
    logins: Optional[str]
    tokens: Optional[str]
    goals_id: Optional[str]
    columns_yandex: Optional[str]

    date_start: Optional[str]
    date_now: Optional[str]

    user_db: Optional[str]
    password_db: Optional[str]
    address_db: Optional[str]
    port_db: Optional[str]
    name_db: Optional[str]

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
    def make_timestamp(date_format: str) -> float:
        return datetime.strptime(date_format, "%Y-%m-%d").timestamp()


settings = Settings()

print(settings.data_headers)
