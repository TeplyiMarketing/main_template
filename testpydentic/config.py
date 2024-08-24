from pydantic import BaseModel

from pydantic_settings import BaseSettings, SettingsConfigDict


class DeepSubModel(BaseModel):
    v4: str


class SubModel(BaseModel):
    v1: str
    v2: bytes
    v3: int
    deep: DeepSubModel


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter='__', env_file="/testpydantic/.env")
    v0: str
    sub_model: SubModel


print(Settings().model_dump())
"""

{
    'v0': '0',
    'sub_model': {'v1': 'json-1', 'v2': b'nested-2', 'v3': 3, 'deep': {'v4': 'v4'}},
}
"""