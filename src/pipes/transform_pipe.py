
from src.repositories.conv_repo import ConvRepository
from src.modules.hash_utils import md5_hex
from src.modules.processors.time_p import utc_to_kst, parse_utc, utc_str_to_kst
from collections import defaultdict

class TransformPipe:
    def __init__(self, conv_repo: ConvRepository):
        self.conv_repo = conv_repo

    def _transform_log(self, day_logs: list[dict]) -> list[dict]:
        """
        API raw logs -> conv table insert용 record list
        반환 record 예:
        {
          "date": "...",
          "qa": "Q" or "A",
          "content": "...",
          "user_id": "...",
          "tenant_id": "...",
          "hash_value": "...",
          "hash_ref": None or "...",
          "date_utc": "..."
        }
        """
        if not day_logs:
            return []
        
        records: list[dict] = []
        for log in day_logs:
            if not all(k in log for k in ("Q", "A", "date", "user_id")):
                continue
            user_id = log["user_id"] if log["user_id"] not in (None, "") else "UNKNOWN"
            tenant_id = log.get("tenant_id")
            if tenant_id not in ("ibk", "ibks"):
                tenant_id = "ibk"

            q_hash = md5_hex(f"{user_id}_{log['Q']}_{log['date']}")
            a_hash = md5_hex(f"{user_id}_{log['A']}_{log['date']}")
            records.append({
                "qa": "Q",
                "content": log['Q'],
                "user_id": user_id,
                "tenant_id": tenant_id,
                "hash_value": q_hash,
                "hash_ref": None,
                "date_utc": log['date'],  # Assuming the date field is already in UTC
            })
            records.append({
                "qa": "A",
                "content": log['A'],
                "user_id": user_id,
                "tenant_id": tenant_id,
                "hash_value": a_hash,
                "hash_ref": q_hash,
                "date_utc": log['date'],  # Assuming the date field is already in UTC
            })
        return records

    def _generate_conv_id(self, records):
        '''
        date_str = record["date"].replace("Z", "+00:00")
        date_value = datetime.fromisoformat(date_str)
        if date_value.tzinfo is None:
            date_value = date_value.replace(tzinfo=timezone.utc)
        kst_date = date_value.astimezone(kst)
        record["date"] = kst_date.isoformat()
        '''
        if not records:
            return records

        # hash → conv_id 미리 조회 (DB 1번)
        hashes = [r["hash_value"] for r in records]
        existing_map = self.conv_repo.get_conv_ids_by_hashes(hashes)   # 반환 예: {hash_value: conv_id}
        
        # 기존 데이터를 필터링하고 새로운 데이터만 처리
        new_records = []
        records.sort(key=lambda r: r["date_utc"])   # 시간순 정렬 (conv_id 안정성)
        
        # 날짜별, tenant별 최대 counter 값을 미리 조회
        date_tenant_counters = {}
        for record in records:
            existing = existing_map.get(record["hash_value"])
            if existing:
                # 기존 데이터는 필터링 (저장하지 않음)
                continue
            
            kst_dt = utc_str_to_kst(record["date_utc"])
            date_key = kst_dt.strftime("%Y%m%d")
            tenant = record.get("tenant_id", "ibk")
            counter_key = (date_key, tenant)
            
            # 최대 counter 값을 조회 (한 번만 조회)
            if counter_key not in date_tenant_counters:
                date_tenant_counters[counter_key] = self.conv_repo.get_max_counter_by_date_tenant(date_key, tenant)
        
        # counter 초기화 (기존 최대값부터 시작)
        counters = defaultdict(int)
        for counter_key, max_counter in date_tenant_counters.items():
            counters[counter_key] = max_counter
        
        # 새로운 데이터에 conv_id 할당
        for record in records:
            existing = existing_map.get(record["hash_value"])
            if existing:
                # 기존 데이터는 필터링 (저장하지 않음)
                continue

            kst_dt = utc_str_to_kst(record["date_utc"])
            date_key = kst_dt.strftime("%Y%m%d")
            tenant = record.get("tenant_id", "ibk")
            counter_key = (date_key, tenant)
            counters[counter_key] += 1
            idx = counters[counter_key]
            record["conv_id"] = f"{date_key}_{tenant}_{idx:05d}"
            new_records.append(record)
        # print(f'len records: {len(new_records)}')
        return new_records
    
    def _add_kst_date(self, records):
        if not records:
            return records
        
        for r in records:
            utc_dt = parse_utc(r["date_utc"])
            kst_dt = utc_to_kst(utc_dt)

            r["date"] = kst_dt.replace(tzinfo=None)
            r["date_utc"] = utc_dt.replace(tzinfo=None)
        return records
        
    def run(self, day_log):
        records = self._transform_log(day_log)
        conv_data = self._generate_conv_id(records)
        conv_data = self._add_kst_date(conv_data)
        return conv_data