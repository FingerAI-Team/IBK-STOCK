class ClsRepository:
    def __init__(self, conn):
        self.conn = conn

    def find_all(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM ibk_stock_cls")
            return cur.fetchall()
        
    def insert(self, conv_id, ensemble):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ibk_stock_cls (conv_id, ensemble) VALUES (%s, %s)",
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
                INSERT INTO ibk_stock_cls (conv_id, ensemble)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                rows
            )
        self.conn.commit()