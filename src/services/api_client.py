import pandas as pd
import requests
import hashlib

class APIPipeline:
    def __init__(self, bearer_tok):
        self.bearer_tok = bearer_tok 

    def get_data_range(self, start_date, end_date, tenant_id='ibk'):
        url = f"https://chat-api.ibks.onelineai.com/api/ibk_securities/admin/logs?tenant_id={tenant_id}"
        # 다음 날을 to_date로 설정 (get_data_api.py와 동일한 방식)
        request_url = f"{url}&from_date_utc={start_date}&to_date_utc={end_date}"
        headers = {
            "Authorization": f"Bearer {self.bearer_tok}"
        }
        print(f"API 요청 URL: {request_url}")
        print(f"Bearer Token: {self.bearer_tok[:10]}..." if self.bearer_tok else "Bearer Token 없음")
        try:
            response = requests.get(request_url, headers=headers)
            print(f"API 응답 상태 코드: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"API 응답 데이터 타입: {type(data)}")
                return data
            else:
                print(f"API 요청 실패: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            print(f"API 요청 중 오류 발생: {str(e)}")
            return []
    
    def get_data(self, date, tenant_id='ibk'):
        url = f"https://chat-api.ibks.onelineai.com/api/ibk_securities/admin/logs?tenant_id={tenant_id}"
        # 다음 날을 to_date로 설정 (get_data_api.py와 동일한 방식)
        from datetime import datetime, timedelta
        from_date = datetime.strptime(date, "%Y-%m-%d")
        end_date = from_date + timedelta(days=1)
        request_url = f"{url}&from_date_utc={from_date}&to_date_utc={end_date}"
        headers = {
            "Authorization": f"Bearer {self.bearer_tok}"
        }
        print(f"API 요청 URL: {request_url}")
        print(f"Bearer Token: {self.bearer_tok[:10]}..." if self.bearer_tok else "Bearer Token 없음")
        try:
            response = requests.get(request_url, headers=headers)
            print(f"API 응답 상태 코드: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"API 응답 데이터 타입: {type(data)}")
                return data
            else:
                print(f"API 요청 실패: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            print(f"API 요청 중 오류 발생: {str(e)}")
            return []
    
    def process_data(self, table_editor, data):
        '''
        api로 받은 데이터를 postgres db에 저장 가능한 형태로 변경 
        api 반환 값: tenant_id, Q, A, date, user_id 
        DB 저장 형태: date, question, answer, user_id, tenant_id, hash_id
        * tenant_id: ibk, ibks  + mtsy (향후 추가 가능) 
        '''
        if not data:
            print("API에서 받은 데이터가 비어있습니다.")
            return pd.DataFrame(columns=["date", "question", "answer", "user_id", "tenant_id", "hash_id"])
        
        records = []
        for d in data:
            if "Q" in d and "A" in d and "date" in d and "user_id" in d:
                hash_value = hashlib.md5(f"{d['user_id']}_{d['Q']}_{d['date']}".encode()).hexdigest()
                hash_id = table_editor.get_hash_id(hash_value)
                records.append({
                    "date": d["date"], 
                    "question": d["Q"],
                    "answer": d["A"],
                    "user_id": d["user_id"], 
                    "tenant_id": d["tenant_id"],
                    "hash_id": hash_id,
                })
            else:
                print(f"데이터 구조가 예상과 다릅니다: {d.keys()}")
        if not records:
            print("처리 가능한 레코드가 없습니다.")
            return pd.DataFrame(columns=["date", "q/a", "content", "user_id", "tenant_id"])
        input_data = pd.DataFrame(records, columns=["date", "question", "answer", "user_id", "tenant_id", "hash_id"])
        print(f"처리된 레코드 수: {len(input_data)}")
        return input_data