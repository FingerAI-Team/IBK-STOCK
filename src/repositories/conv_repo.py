class ConvRepository:
    def __init__(self, conn):
        self.conn = conn

    def find_all(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM ibk_convlog")
            return cur.fetchall()

    def find_by_date(self, date: str):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM ibk_convlog
                WHERE SPLIT_PART(conv_id, '_', 1) = %s
                """,
                (date,)
            )
            return cur.fetchall()
    
    def get_conv_id_by_hash(self, hash_value):
        query = """
        SELECT conv_id
        FROM ibk_convlog
        WHERE hash_value = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (hash_value,))
            result = cur.fetchone()
        return result[0] if result else None

    def exists(self, conv_id: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM ibk_convlog WHERE conv_id = %s)",
                (conv_id,)
            )
            return cur.fetchone()[0]

    def get_conv_ids_by_hashes(self, hashes):
        if not hashes:
            return {}
        query = """
            SELECT hash_value, conv_id
            FROM ibk_convlog
            WHERE hash_value = ANY(%s)
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (hashes,))
            rows = cur.fetchall()
        # {hash_value: conv_id}
        return {row[0]: row[1] for row in rows}
    
    def get_max_counter_by_date_tenant(self, date_key: str, tenant: str) -> int:
        """
        특정 날짜와 tenant의 최대 counter 값을 조회합니다.
        conv_id 형식: {date_key}_{tenant}_{counter:05d}
        """
        query = """
            SELECT conv_id
            FROM ibk_convlog
            WHERE conv_id LIKE %s
            ORDER BY conv_id DESC
            LIMIT 1
        """
        pattern = f"{date_key}_{tenant}_%"
        with self.conn.cursor() as cur:
            cur.execute(query, (pattern,))
            row = cur.fetchone()
            if row:
                conv_id = row[0]
                # conv_id에서 counter 추출: {date_key}_{tenant}_{counter}
                parts = conv_id.split('_')
                if len(parts) >= 3:
                    try:
                        return int(parts[2])
                    except ValueError:
                        return 0
            return 0
    
    def get_max_counters_batch(self, date_tenant_pairs: list[tuple]) -> dict:
        """
        여러 날짜와 tenant 조합의 최대 counter 값을 한 번에 조회합니다.
        args:
            date_tenant_pairs: [(date_key, tenant), ...] 형태의 리스트
        returns:
            {(date_key, tenant): max_counter} 형태의 딕셔너리
        """
        if not date_tenant_pairs:
            return {}
        
        # 한 번의 쿼리로 모든 최대값 조회
        # PostgreSQL의 DISTINCT ON을 사용하여 각 조합의 최대값만 조회
        conditions = []
        params = []
        for date_key, tenant in date_tenant_pairs:
            conditions.append("(SPLIT_PART(conv_id, '_', 1) = %s AND SPLIT_PART(conv_id, '_', 2) = %s)")
            params.extend([date_key, tenant])
        
        query = """
            SELECT DISTINCT ON (SPLIT_PART(conv_id, '_', 1), SPLIT_PART(conv_id, '_', 2))
                   conv_id,
                   SPLIT_PART(conv_id, '_', 1) as date_key,
                   SPLIT_PART(conv_id, '_', 2) as tenant
            FROM ibk_convlog
            WHERE (
        """ + " OR ".join(conditions) + """
            )
            ORDER BY SPLIT_PART(conv_id, '_', 1), SPLIT_PART(conv_id, '_', 2), conv_id DESC
        """
        
        result = {}
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            for row in rows:
                conv_id, date_key, tenant = row
                parts = conv_id.split('_')
                if len(parts) >= 3:
                    try:
                        counter = int(parts[2])
                        key = (date_key, tenant)
                        result[key] = counter
                    except ValueError:
                        pass
        
        # 조회되지 않은 조합은 0으로 초기화
        for date_key, tenant in date_tenant_pairs:
            key = (date_key, tenant)
            if key not in result:
                result[key] = 0
        
        return result

    def insert_one(self, row: tuple):
        """
        row = (conv_id, date, qa, content, user_id, tenant_id, hash_value, hash_ref, date_utc)
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ibk_convlog
                (conv_id, date, qa, content, user_id, tenant_id, hash_value, hash_ref, date_utc)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                row
            )
        self.conn.commit()

    def insert_many(self, rows: list[tuple]):
        if not rows:
            return
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO ibk_convlog
                (conv_id, date, qa, content, user_id, tenant_id, hash_value, hash_ref, date_utc)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                rows
            )
        self.conn.commit()