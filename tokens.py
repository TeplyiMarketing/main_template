from amocrm.auth import auth, get_access_token
from data.config_vars import Config
from data.dates import update_dates_amocrm, update_dates_yandex

config = Config()
config.load_from_env('.env')

if __name__ == "__main__":
    update_dates_amocrm()
    update_dates_yandex()
    auth(config.link_access, config.client, config.client_secret, config.refresh_token, config.redirect_uri)
    get_access_token(config.link_access, config.client, config.client_secret, config.refresh_token, config.redirect_uri)