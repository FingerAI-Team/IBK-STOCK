
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
        }
        """
        if not day_logs:
            return []
        
        records: list[dict] = [] 
        for r in day_logs:
            if not all(k in r for k in ("Q", "A", "date", "user_id")):
                continue
            user_id = r["user_id"] if r["user_id"] not in (None, "") else "UNKNOWN"
            tenant_id = r.get("tenant_id") or "ibk"
            if tenant_id not in ("ibk", "ibks"):
                tenant_id = "ibk"

            q_hash = md5_hex(f"{user_id}_{r['Q']}_{r['date']}")
            a_hash = md5_hex(f"{user_id}_{r['A']}_{r['date']}")
            records.append({
                "date": r['date'],
                "qa": "Q",
                "content": r['Q'],
                "user_id": user_id,
                "tenant_id": tenant_id,
                "hash_value": q_hash,
                "hash_ref": None,
            })
            records.append({
                "date": r['date'],
                "qa": "A",
                "content": r['A'],
                "user_id": user_id,
                "tenant_id": tenant_id,
                "hash_value": a_hash,
                "hash_ref": q_hash,
            })
        return records

    def _generate_conv_id(self, records):
        if not records:
            return records

        kst = timezone(timedelta(hours=9))
        # hash → conv_id 미리 조회 (DB 1번)
        hashes = [r["hash_value"] for r in records]
        existing_map = self.conv_repo.get_conv_ids_by_hashes(hashes)   # 반환 예: {hash_value: conv_id}
        counters = defaultdict(int)
        records.sort(key=lambda r: r["date"])   # 시간순 정렬 (conv_id 안정성)

        for record in records:
            existing = existing_map.get(record["hash_value"])
            if existing:
                record["conv_id"] = existing
                continue

            # UTC → KST
            date_str = record["date"].replace("Z", "+00:00")
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
        return records
    
    def run(self, day_log):
        records = self._transform_log(day_log)
        conv_data = self._generate_conv_id(records)
        return conv_data