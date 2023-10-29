import requests as requests
from dotenv import set_key
from logs.logging import logger


def get_access_token(link_access, client, client_secret, refresh_token, redirect_uri):
    data = {"client_id": client, "client_secret": client_secret, "grant_type": 'authorization_code',
            "code": refresh_token, "redirect_uri": redirect_uri}
    response = requests.post(link_access, data=data)
    if response.status_code == 200:
        try:
            access_token = response.json()['access_token']
            set_key('.env', 'ACCESS_TOKEN', access_token)
            refresh_code = response.json()['refresh_token']
            set_key('.env', 'REFRESH_TOKEN', refresh_code)
            logger.info(response.status_code)
        except KeyError:
            logger.warning('Key not found - pass. File auth.')
    else:
        logger.info(response.status_code)
        logger.info(response.json())


def auth(link_access, client, client_secret, refresh_token, redirect_uri):
    data = {"client_id": client, "client_secret": client_secret, "grant_type": 'refresh_token',
            "refresh_token": refresh_token, "redirect_uri": redirect_uri}
    response = requests.post(link_access, data=data)
    if response.status_code == 200:
        try:
            access_token = response.json()['access_token']
            set_key('.env', 'ACCESS_TOKEN', access_token)
            refresh = response.json()['refresh_token']
            set_key('.env', 'REFRESH_TOKEN', refresh)
            logger.info(response.status_code)
        except KeyError:
            logger.warning('Key not found - pass. File auth.')
    else:
        logger.info(response.status_code)
        logger.info(response.json())
