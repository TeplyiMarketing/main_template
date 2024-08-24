import pytest

from amocrm.auth import authorization
from config import settings

@pytest.fixture(scope="session", autouse=True)
def initialize():
    print(f"{settings.mode}")
    assert settings.mode == "TEST"


# def test_config():
#     print(settings.amocrm.model_dump())

# def test_date():
#     print(datetime.date.today())

def test_auth():
    assert authorization(env_path=settings.env_path, link_access=settings.link_access_amocrm,
                         response_data=settings.data_for_request_amocrm) == 200

# def test_update_dates():
#     update_dates()

