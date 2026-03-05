from src.repositories.clicked_repo import ClickedRepository
from src.repositories.hash_repo import HashRepository
from src.repositories.conv_repo import ConvRepository
from src.repositories.cls_repo import ClsRepository
from src.repositories.core import DBConnection
from src.modules.hash_utils import generate_hash
from datetime import datetime, timezone, timedelta
import pandas as pd

class StorePipe:
    def __init__(self, db_connection: DBConnection):
        self.conv_repo = ConvRepository(db_connection)
        self.cls_repo = ClsRepository(db_connection)
        self.clicked_repo = ClickedRepository(db_connection)
        self.hash_repo = HashRepository(db_connection)

    def transform_log(self, day_log):
        pass

    def generate_conv_id(self, records):
        date_counters = {}
        for record in records:
            date_value = datetime.fromisoformat(record["date"])
            kst = timezone(timedelta(hours=9))
            if date_value.tzinfo is None:
                date_value = date_value.replace(tzinfo=timezone.utc)
            kst_date = date_value.astimezone(kst)
            pk_date = f"{kst_date.year}{str(kst_date.month).zfill(2)}{str(kst_date.day).zfill(2)}"
            if pk_date not in date_counters:
                max_conv = self.conv_repo.get_max_conv_id(pk_date)
                if max_conv:
                    date_counters[pk_date] = int(max_conv.split("_")[1])
                else:
                    date_counters[pk_date] = 0
            date_counters[pk_date] += 1
            record["conv_id"] = f"{pk_date}_{str(date_counters[pk_date]).zfill(5)}"
            record["date"] = kst_date.isoformat()
        return records
    
    def filter_duplicates(self, records):
        filtered = []
        for r in records:
            if self.conv_repo.exists_hash(r["hash_value"]):
                continue
            filtered.append(r)
        return filtered
    
    def process_log(self, day_log):
        '''
        원라인에서 API로 받은 로그를 conv table에 저장 가능한 데이터로 변환
        date, qa, content, user_id, tenant_id, hash_value, hash_ref: ibk, msty 
        '''
        if not log:
            print("API에서 받은 데이터가 비어있습니다.")
            return pd.DataFrame(columns=["date", "q/a", "content", "user_id", "tenant_id", "hash_value", "hash_ref"])
            
        records = []
        for log in day_log:
            if "Q" in log and "A" in log and "date" in log and "user_id" in log:
                # user_id가 None인 경우 'UNKNOWN'으로 처리
                user_id = log["user_id"] if log["user_id"] is not None and log["user_id"] != "" else "UNKNOWN"
                tenant_id = log.get("tenant_id") if log.get("tenant_id") is not None else None
                
                # Q와 A의 해시값을 미리 생성 (user_id가 None이면 'UNKNOWN' 사용)
                q_hash = generate_hash(user_id, log["Q"], log["date"])
                a_hash = generate_hash(user_id, log["A"], log["date"])
                
                records.append({
                    "date": log["date"], 
                    "q/a": "Q", 
                    "content": log["Q"], 
                    "user_id": user_id,  # None이면 'UNKNOWN'으로 저장
                    "tenant_id": tenant_id,
                    "hash_value": q_hash,
                    "hash_ref": None  # Q는 hash_ref가 NULL
                })
                records.append({
                    "date": log["date"], 
                    "q/a": "A", 
                    "content": log["A"], 
                    "user_id": user_id,  # None이면 'UNKNOWN'으로 저장
                    "tenant_id": tenant_id,
                    "hash_value": a_hash,
                    "hash_ref": q_hash  # A는 Q의 hash_value를 hash_ref로
                })
            else:
                print(f"데이터 구조가 예상과 다릅니다: {log.keys()}")
        if not records:
            print("처리 가능한 레코드가 없습니다.")
            return pd.DataFrame(columns=["date", "q/a", "content", "user_id", "tenant_id", "hash_value", "hash_ref"])
            
        input_data = pd.DataFrame(records, columns=["date", "q/a", "content", "user_id", "tenant_id", "hash_value", "hash_ref"])
        print(f"처리된 레코드 수: {len(input_data)}")
        return input_data
    
    def store(self, conv_data, cls_data, clicked_data, hash_data):
        self.conv_repo.insert(conv_data)
        self.cls_repo.insert(cls_data)
        self.clicked_repo.insert(clicked_data)
        self.hash_repo.insert(hash_data)