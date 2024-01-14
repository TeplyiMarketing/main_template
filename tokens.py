from amocrm.auth import auth, get_access_token
from data.config import Config
from data.dates import update_dates_amocrm, update_dates_yandex

config = Config()
config.load_from_env('.env')
amocrm_config = config.get('amocrm')

if __name__ == "__main__":
    update_dates_amocrm()
    update_dates_yandex()
    auth(subdomain=amocrm_config.subdomain, client_id=amocrm_config.client_id,
         client_secret=amocrm_config.client_secret,
         redirect_uri=amocrm_config.redirect_uri, refresh_token=amocrm_config.refresh_token)
    get_access_token(subdomain=amocrm_config.subdomain, client_id=amocrm_config.client_id,
                     client_secret=amocrm_config.client_secret,
                     redirect_uri=amocrm_config.redirect_uri, refresh_token=amocrm_config.refresh_token)
