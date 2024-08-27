import requests as requests

from config import settings
from logs.logging import logger


def refresh_token(response) -> int:
    """
    Функция нужна для считывания токенов и обновления их в .env файле
    :param response: Сделанный пост запрос с помощью requests.
    :return: Статус код ответа
    """
    if response.status_code == 200 and response.json()['access_token'] and response.json()['refresh_token']:
        logger.debug("Попытка обновления при статусе 200")
        settings.update_variable_env(variables=['ACCESS_TOKEN', 'REFRESH_TOKEN'],
                                     new_variables=[response.json()['access_token'], response.json()['refresh_token']])
        logger.trace(f"Попытка обновления при статусе {response.status_code} - {response.json()}")
        return response.status_code


def authorization(link_access: str, response_data: dict) -> int:
    response_to_amo = requests.post(link_access, data=response_data)
    if response_to_amo.status_code == 200:
        return refresh_token(response_to_amo)

    if (response_to_amo.status_code == 400 and response_to_amo.json()['hint'] == "Authorization code has been revoked"
            and response_to_amo.json()['hint'] != "Cannot decrypt the authorization code"):
        logger.info("Попытка обновления токена...")
        response_data["grant_type"] = "refresh_token"
        response_data["refresh_token"], response_data["redirect_uri"] = (
            response_data.pop("code"), response_data.pop("redirect_uri"))
        response_400 = requests.post(link_access, data=response_data)
        if response_400.status_code == 401:
            logger.error(f"Попытка обновления не удалась ошибка - {response_400.json()["title"]}")
            logger.trace(f"Данные в случае ошибки - {response_data}")
            return 401
        else:
            logger.success("Обновление выполнено!")
            return refresh_token(response_400)
    else:
        logger.error("Токен не валидный, пожалуйста обновите его и повторите попытку!")
        return response_to_amo.status_code
