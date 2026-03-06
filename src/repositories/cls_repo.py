class ClsRepository:
    def __init__(self, conn):
        self.conn = conn

    def find_all(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM cls_table")
            return cur.fetchall()
        
    def insert(self, conv_id, ensemble):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cls_table (conv_id, ensemble) VALUES (%s, %s)",
                (conv_id, ensemble)
            )

    def insert_many(self, rows: list[tuple]):
        """
        rows: [(conv_id, ensemble), ...]
        """
        if not rows:
            return
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO cls_table (conv_id, ensemble)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                rows
            )
        self.conn.commit()