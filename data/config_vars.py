from datetime import datetime

from sqlalchemy import create_engine

from data.config import load_config


class Config:
    def __init__(self):
        # Amocrm
        self.subdomain = None
        self.client = None
        self.client_secret = None
        self.redirect_uri = None
        self.refresh_token = None
        self.bearer = None
        self.access_token = None
        self.link_access = None
        self.link_leads = None
        self.link_events = None
        self.data_headers = None
        # AmoDates
        self.now_date_amo = None
        self.last_date_amo = None
        self.timestamp_amo_now = None
        self.timestamp_amo_last = None
        # Database
        self.link_postgres = None
        self.engine = None
        # YandexDates
        self.now_date_yandex = None
        self.last_date_yandex = None
        # Yandex
        self.reports_url = None
        self.token_type = None
        self.token_yandex = None
        self.login_yandex = None
        # YandexDatas
        self.goals_id = None
        self.columns_yandex = None
        # Others
        self.reports_url = None
        self.timestamp_now = None

    def load_from_env(self, env_path):
        config = load_config(env_path)
        # Amocrm
        self.subdomain = config.others.subdomain
        self.client = config.gettoken.client_id
        self.client_secret = config.gettoken.client_secret
        self.redirect_uri = config.gettoken.redirect_uri
        self.bearer = config.auth_crm.bearer
        self.refresh_token = config.auth_crm.refresh_token
        self.access_token = config.auth_crm.access_token
        self.now_date_amo = config.amo_dates.now_date_amo
        self.last_date_amo = config.amo_dates.last_date_amo
        self.now_date_yandex = config.yandex_dates.now_date_yandex
        self.last_date_yandex = config.yandex_dates.last_date_yandex
        self.timestamp_amo_now = datetime.strptime(self.now_date_amo, "%Y-%m-%d").timestamp()
        self.timestamp_amo_last = datetime.strptime(self.last_date_amo, "%Y-%m-%d").timestamp()
        self.link_access = f'https://{self.subdomain}.amocrm.ru/oauth2/access_token'
        self.link_leads = f'https://{self.subdomain}.amocrm.ru/api/v4/leads?filter[created_at][from]={self.timestamp_amo_last}&filter[created_at][to]={self.timestamp_amo_now}'
        self.link_events = f'https://{self.subdomain}.amocrm.ru/api/v4/events?filter[entity][]=lead&filter[type]=lead_status_changed&filter[created_at][from]={self.timestamp_amo_last}&filter[created_at][to]={self.timestamp_amo_now}'
        self.data_headers = {"Authorization": f'{self.bearer} ' + self.access_token}
        # Database
        self.link_postgres = f'postgresql://{config.db.user_db}:{config.db.password_db}@{config.db.address_db}:{config.db.port_db}/{config.db.name_db}'
        self.engine = create_engine(self.link_postgres)
        # Yandex
        self.reports_url = config.reports.token_type
        self.token_type = config.reports.token_type
        self.token_yandex = config.yandex.token_yandex
        self.login_yandex = config.yandex.login_yandex
        # YandexDatas
        self.goals_id = config.yandex_datas.goals_id
        self.columns_yandex = config.yandex_datas.columns_yandex
        # Others
        self.reports_url = config.reports.reports_url
