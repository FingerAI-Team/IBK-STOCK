class ClickedRepository:
    def __init__(self, conn):
        self.conn = conn

    def find_all(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM clicked_table")
            return cur.fetchall()

    def insert(self, conv_id, clicked, user_id):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO clicked_table (conv_id, clicked, user_id)
                VALUES (%s, %s, %s)
                """,
                (conv_id, clicked, user_id)
            )