from loguru import logger

# Настройка Loguru для записи всех логов в общий файл
logger.add("logs/my_logs.log", rotation="150 MB", retention="7 days", level="DEBUG")
