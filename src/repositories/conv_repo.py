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

    def exists(self, conv_id: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM conv_table WHERE conv_id = %s)",
                (conv_id,)
            )
            return cur.fetchone()[0]

    def insert(self, conv_data: tuple):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO conv_table
                (conv_id, date, question, answer, user_id, tenant_id, hash_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                conv_data
            )