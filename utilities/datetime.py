from _datetime import datetime, timedelta, timezone as datetime_timezone
from pytz import timezone


def now(date_timezone: str = 'UTC') -> datetime:
    if 'utc' == date_timezone.lower():
        return datetime.utcnow().replace(tzinfo=timezone('UTC'))

    if 'local' == date_timezone.lower():
        return datetime.now().replace(tzinfo=timezone(current_timezone()))

    return datetime.now(timezone(date_timezone))


def get_timezone(timezone_string: str) -> timezone:
    return timezone(timezone_string)

def current_timezone() -> str:
    return str(datetime.now(datetime_timezone(timedelta())).astimezone().tzinfo)
