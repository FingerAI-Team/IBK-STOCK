
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from src.repositories.conv_repo import ConvRepository
from src.modules.hash_utils import md5_hex

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

        kst = timezone(timedelta(hours=9))
        # hash → conv_id 미리 조회 (DB 1번)
        hashes = [r["hash_value"] for r in records]
        existing_map = self.conv_repo.get_conv_ids_by_hashes(hashes)   # 반환 예: {hash_value: conv_id}
        counters = defaultdict(int)
        records.sort(key=lambda r: r["date_utc"])   # 시간순 정렬 (conv_id 안정성)

        for record in records:
            existing = existing_map.get(record["hash_value"])
            if existing:
                record["conv_id"] = existing
                continue

            # UTC → KST
            date_str = record["date_utc"].replace("Z", "+00:00")
            utc_dt = datetime.fromisoformat(date_str)
            if utc_dt.tzinfo is None:
                utc_dt = utc_dt.replace(tzinfo=timezone.utc)
            kst_dt = utc_dt.astimezone(kst)
            date_key = kst_dt.strftime("%Y%m%d")
            tenant = record.get("tenant_id", "ibk")
            counter_key = (date_key, tenant)
            counters[counter_key] += 1
            idx = counters[counter_key]
            record["conv_id"] = f"{date_key}_{tenant}_{idx:05d}"
        # print(f'len records: {len(records)}')
        return records
    
    def _add_kst_date(self, records):
        if not records:
            return records
        kst = timezone(timedelta(hours=9))
        for r in records:
            date_str = r["date_utc"].replace("Z", "+00:00")
            utc_dt = datetime.fromisoformat(date_str)
            if utc_dt.tzinfo is None:
                utc_dt = utc_dt.replace(tzinfo=timezone.utc)
            kst_dt = utc_dt.astimezone(kst)
            r["date"] = kst_dt.replace(tzinfo=None)
            r["date_utc"] = utc_dt.replace(tzinfo=None)
        return records
        
    def run(self, day_log):
        records = self._transform_log(day_log)
        conv_data = self._generate_conv_id(records)
        conv_data = self._add_kst_date(conv_data)
        return conv_data
