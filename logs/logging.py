from loguru import logger


def trace_only(record):
    return record["level"].name == "TRACE"

logger.add("logs.log",
           rotation="150 MB",
           retention="7 days",
           level="TRACE",
           format="{time:YYYY-MM-DD} | {time:HH:mm} | {level} | {message}",
           filter=trace_only
           )
