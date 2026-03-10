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
        print(f'Inserting {len(rows)} rows into ibk_convlog')
        print(f'Sample row: {rows[0] if rows else "No rows to insert"}')
        print(f'Sample row: {rows[-1] if rows else "No rows to insert"}')
        if not rows:
            return
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO ibk_convlog
                (conv_id, date, qa, content, user_id, tenant_id, hash_value, hash_ref, date_utc)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (conv_id, qa) DO NOTHING
                """,
                rows
            )
        self.conn.commit()