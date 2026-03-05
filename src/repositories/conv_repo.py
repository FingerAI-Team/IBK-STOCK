class ConvRepository:
    def __init__(self, conn):
        self.conn = conn

    def find_all(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM conv_table")
            return cur.fetchall()

    def find_by_date(self, date: str):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM conv_table
                WHERE SPLIT_PART(conv_id, '_', 1) = %s
                """,
                (date,)
            )
            return cur.fetchall()
        
    def get_max_conv_id(self, date_prefix):
        start = f"{date_prefix}_00000"
        end = f"{date_prefix}_99999"
        query = """
        SELECT MAX(conv_id)
        FROM ibk_convlog
        WHERE conv_id >= %s
        AND conv_id < %s
        """
        self.cur.execute(query, (start, end))
        result = self.cur.fetchone()
        return result[0] if result else None

    def exists(self, conv_id: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM conv_table WHERE conv_id = %s)",
                (conv_id,)
            )
            return cur.fetchone()[0]

    def insert_one(self, row: tuple):
        """
        row = (conv_id, date, qa, content, user_id, tenant_id, hash_value, hash_ref)
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO conv_table
                (conv_id, date, qa, content, user_id, tenant_id, hash_value, hash_ref)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
                INSERT INTO conv_table
                (conv_id, date, qa, content, user_id, tenant_id, hash_value, hash_ref)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (hash_value) DO NOTHING
                """,
                rows
            )
        self.conn.commit()