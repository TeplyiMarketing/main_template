from dataclasses import dataclass

from environs import Env


@dataclass
class AmoData:
    subdomain: str
    client_id: str
    client_secret: str
    redirect_uri: str
    access_token: str
    refresh_token: str


@dataclass
class YandexData:
    tokens: list
    logins: list
    reports_url: str
    goals_id: list
    columns: list


@dataclass
class Dates:
    finish_amo_leads: str
    start_amo_leads: str
    finish_amo_events: str
    start_amo_events: str
    finish_yandex_date: str
    start_yandex_date: str


@dataclass
class DbConfig:
    user_db: str
    password_db: str
    address_db: str
    port_db: str
    name_db: str


def load_config(path: str = None):
    # Загрузить данные конфигурации из файла .env.dist2
    env = Env()
    env.read_env(path)

    return {
        'amocrm': AmoData(
            subdomain=env.str("SUBDOMAIN"),
            client_id=env.str("CLIENT_ID"),
            client_secret=env.str("CLIENT_SECRET"),
            redirect_uri=env.str("REDIRECT_URI"),
            refresh_token=env.str("REFRESH_TOKEN"),
            access_token=env.str("ACCESS_TOKEN"),
        ),
        'yandex': YandexData(
            reports_url=env.str("REPORTS_URL"),
            tokens=env.list("TOKENS"),
            logins=env.list("LOGINS"),
            goals_id=env.list('GOALS_ID'),
            columns=env.list('COLUMNS'),
        ),
        'dates': Dates(
            finish_amo_leads=env.str("FINISH_AMO_LEADS"),
            start_amo_leads=env.str("START_AMO_LEADS"),
            finish_amo_events=env.str("FINISH_AMO_EVENTS"),
            start_amo_events=env.str("START_AMO_EVENTS"),
            finish_yandex_date=env.str("FINISH_YANDEX_DATE"),
            start_yandex_date=env.str("START_YANDEX_DATE"),
        ),
        'db': DbConfig(
            user_db=env.str('USER_DB'),
            password_db=env.str('PASSWORD_DB'),
            address_db=env.str('ADDRESS_DB'),
            port_db=env.str('PORT_DB'),
            name_db=env.str('NAME_DB')
        ),
    }


class Config:
    def __init__(self):
        self.config_data = None

    def load_from_env(self, env_path):
        self.config_data = load_config(env_path)

    def get(self, key, default=None):
        return self.config_data.get(key, default)
