import re

def remove_keywords(df, col, keyword=None, exceptions=None):
    if exceptions != None: 
        if keyword != None:
            # pattern = r'(?<![\w가-힣])(?:' + '|'.join(map(re.escape, keyword)) + r')(?![\w가-힣])'
            pattern = rf'(?<![0-9A-Za-z가-힣]){re.escape(keyword)}(?![0-9A-Za-z가-힣])'
        else:
            pattern = r'(?<![\w가-힣])(\S*주)(?![\w가-힣])'    # 테마주 같은 함정 증권 종목 제거 
        mask = df[col].str.contains(pattern, na=False) & ~df[col].str.contains('|'.join(map(re.escape, exceptions)), na=False)
        df = df[~mask]
        return df.reset_index(drop=True)
    else: 
        keyword_idx = df[df[col].str.contains(keyword, na=False)].index 
        df.drop(keyword_idx, inplace=True)
        return df.reset_index(drop=True)
    
def remove_duplications(text):
    '''
    줄바꿈 문자를 비롯한 특수 문자 중복을 제거합니다.
    '''
    text = re.sub(r'(\n\s*)+\n+', '\n\n', text)    # 다중 줄바꿈 문자 제거
    text = re.sub(r"\·{1,}", " ", text)    
    return re.sub(r"\.{1,}", ".", text)
    
def remove_patterns(text, pattern):
    '''
    입력된 패턴을 제거합니다. 
    pattern:
    r'^\d+\.\s*': "숫자 + ." 
    r"(뉴스|주식|정보|분석)$": 삼성전자뉴스 -> 삼성전자
    '''
    return re.sub(pattern, '', text)