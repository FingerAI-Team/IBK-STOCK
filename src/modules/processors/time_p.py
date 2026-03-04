from datetime import datetime, timedelta

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