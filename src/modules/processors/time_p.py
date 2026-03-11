from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
def get_previous_day_date():
    '''
    전일 연도, 월, 일을 반환합니다.   
    returns: 
    20240103
    '''
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    return str(yesterday.year), str(yesterday.month).zfill(2), str(yesterday.day).zfill(2)

def get_current_date():
    '''
    현재 연도, 월, 일을 반환합니다.
    '''
    now = datetime.now()
    return str(now.year), str(now.month).zfill(2), str(now.day).zfill(2)

def parse_utc(dt_str: str) -> datetime:
    """
    ISO datetime string -> UTC datetime
    """
    dt_str = dt_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(dt_str)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt

def utc_to_kst(dt: datetime) -> datetime:
    """
    UTC datetime -> KST datetime
    """
    return dt.astimezone(KST)

def utc_str_to_kst(dt_str: str) -> datetime:
    """
    ISO datetime string -> KST datetime
    """
    return utc_to_kst(parse_utc(dt_str))