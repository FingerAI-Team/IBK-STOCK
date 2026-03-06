
from datetime import datetime, timezone, timedelta
from src.modules.hash_utils import md5_hex

class TransformPipe:
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
        date_counters = {}
        kst = timezone(timedelta(hours=9))

        for record in records:
            date_str = record["date"].replace("Z", "+00:00")
            date_value = datetime.fromisoformat(date_str) 
            if date_value.tzinfo is None:
                date_value = date_value.replace(tzinfo=timezone.utc)
            kst_date = date_value.astimezone(kst)
            pk_date = kst_date.strftime("%Y%m%d")
            if pk_date not in date_counters:
                max_conv = self.conv_repo.get_max_conv_id(pk_date)
                if max_conv:
                    try:
                        date_counters[pk_date] = int(max_conv.split("_")[1])
                    except Exception:
                        date_counters[pk_date] = 0
                else:
                    date_counters[pk_date] = 0
            date_counters[pk_date] += 1
            record["conv_id"] = f"{pk_date}_{date_counters[pk_date]:05d}"
            record["date"] = kst_date.isoformat()
        return records
    
    def run(self, day_log):
        records = self._transform_log(day_log)
        conv_data = self._generate_conv_id(records)
        return conv_data