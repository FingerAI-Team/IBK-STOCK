import re

def check_expr(expr, text):
    '''
    expr 값이 text에 있는지 검사합니다. 있다면 True를, 없다면 False를 반환합니다. 
    '''
    return bool(re.search(expr, text))

def get_val_with_indices(val, text):
    '''
    val 값이 text에 있으면 시작과 끝 위치 정보와 함께 값을 반환합니다.
    '''
    found_stocks = []
    if isinstance(val, str):
        pattern = rf'(^|[^a-zA-Z0-9가-힣]){re.escape(val)}($|[^a-zA-Z0-9가-힣])'
        matches = list(re.finditer(pattern, text))
        for match in matches:
            # 매칭된 텍스트에서 실제 단어의 시작과 끝 위치 조정
            start = match.start() + (1 if match.group(1) else 0)
            end = match.end() - (1 if match.group(2) else 0)
            found_stocks.append((val, start, end))
        return found_stocks
    elif isinstance(val, list):
        pattern = re.compile(r'\b(?:' + '|'.join(map(re.escape, val)) + r')\b')
        matches = [(match.group(), match.start(), match.end()) for match in pattern.finditer(text)]
        return matches