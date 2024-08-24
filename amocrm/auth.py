import json

import requests as requests
from dotenv import set_key

from logs.logging import logger


def refresh_token(env_path: str, response: json) -> int:
    if response.status_code == 200 and response.json()['access_token'] and response.json()['refresh_token']:
        logger.debug("Попытка обновления при статусе 200")
        logger.trace(f"Попытка обновления при статусе {response.status_code} - {response.json()}")
        set_key(env_path, 'REFRESH_TOKEN', response.json()['refresh_token'])
        set_key(env_path, 'ACCESS_TOKEN', response.json()['access_token'])
        return response.status_code


def authorization(env_path: str, link_access: str, response_data: dict) -> int:
    logger.trace(f"Входные данные - {response_data}")
    response_to_amo = requests.post(link_access, data=response_data)
    if response_to_amo.status_code == 200:
        return refresh_token(env_path, response_to_amo)

    if (response_to_amo.status_code == 400 and response_to_amo.json()['hint'] == "Authorization code has been revoked"
            and response_to_amo.json()['hint'] != "Cannot decrypt the authorization code"):
        logger.info("Попытка обновления токена...")
        response_data["grant_type"] = "refresh_token"
        response_data["refresh_token"] = response_data.pop("code")
        response_data["redirect_uri"] = response_data.pop("redirect_uri")
        response_400 = requests.post(link_access, data=response_data)
        if response_400.status_code == 401:
            logger.error(f"Попытка обновления не удалась ошибка - {response_400.json()["title"]}")
            logger.trace(f"Данные в случае ошибки - {response_data}")
            return 401
        else:
            logger.success("Обновление выполнено!")
            return refresh_token(env_path, response_400)
    else:
        logger.error("Токен не валидный, пожалуйста обновите его и повторите попытку!")
        return response_to_amo.status_code
