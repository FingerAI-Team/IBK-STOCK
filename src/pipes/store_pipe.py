from src.repositories.conv_repo import ConvRepository
from src.repositories.cls_repo import ClsRepository

class StorePipe:
    def __init__(self, conv_repo: ConvRepository, cls_repo: ClsRepository):
        self.conv_repo = conv_repo
        self.cls_repo = cls_repo

    def _to_conv_rows(self, records: list[dict]) -> list[tuple]:
        return [
            (
                r["conv_id"],
                r["date"],
                r["qa"],
                r["content"],
                r["user_id"],
                r["tenant_id"],
                r["hash_value"],
                r["hash_ref"],
                r["date_utc"]
            )
            for r in records
        ]
    
    def run(self, conv_records, cls_data):
        conv_rows = self._to_conv_rows(conv_records)
        self.conv_repo.insert_many(conv_rows)
        self.cls_repo.insert_many(cls_data)        