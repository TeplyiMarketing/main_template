from dataclasses import dataclass

from environs import Env


@dataclass
class GetToken:
    client_id: str
    client_secret: str
    redirect_uri: str


@dataclass
class AuthCRM:
    bearer: str
    access_token: str
    refresh_token: str


@dataclass
class AmoDates:
    now_date_amo: str
    last_date_amo: str


@dataclass
class DbConfig:
    user_db: str
    password_db: str
    address_db: str
    port_db: str
    name_db: str


@dataclass
class Others:
    subdomain: str


@dataclass
class Reports:
    reports_url: str
    token_type: str


@dataclass
class YandexDates:
    now_date_yandex: str
    last_date_yandex: str


@dataclass
class YandexToken:
    token_yandex: str
    login_yandex: str


@dataclass
class YandexDatas:
    goals_id: list
    columns_yandex: list


@dataclass
class Config:
    gettoken: GetToken
    auth_crm: AuthCRM
    amo_dates: AmoDates
    db: DbConfig
    others: Others
    reports: Reports
    yandex_dates: YandexDates
    yandex: YandexToken
    yandex_datas: YandexDatas


def load_config(path: str = None):
    # Загрузить данные конфигурации из файла .env
    env = Env()
    env.read_env(path)

    return Config(
        gettoken=GetToken(
            client_id=env.str("CLIENT_ID"),
            client_secret=env.str("CLIENT_SECRET"),
            redirect_uri=env.str("REDIRECT_URI")
        ),
        auth_crm=AuthCRM(
            bearer=env.str("TOKEN_TYPE"),
            access_token=env.str("ACCESS_TOKEN"),
            refresh_token=env.str("REFRESH_TOKEN"),
        ),
        amo_dates=AmoDates(
            now_date_amo=env.str("NOW_DATE_AMO"),
            last_date_amo=env.str("LAST_DATE_AMO"),
        ),
        db=DbConfig(
            user_db=env.str('USER_DB'),
            password_db=env.str('PASSWORD_DB'),
            address_db=env.str('ADDRESS_DB'),
            port_db=env.str('PORT_DB'),
            name_db=env.str('NAME_DB')
        ),
        others=Others(
            subdomain=env.str("SUBDOMAIN"),
        ),
        reports=Reports(
            reports_url=env.str("REPORTS_URL"),
            token_type=env.str("TOKEN_TYPE"),
        ),
        yandex_dates=YandexDates(
            now_date_yandex=env.str("NOW_DATE_YANDEX"),
            last_date_yandex=env.str("LAST_DATE_YANDEX"),
        ),
        yandex=YandexToken(
            token_yandex=env.str("TOKEN_YANDEX"),
            login_yandex=env.str("LOGIN_YANDEX"),
        ),
        yandex_datas=YandexDatas(
            goals_id=env.list('GOALS_ID'),
            columns_yandex=env.list('COLUMNS_YANDEX'),
        )
    )
