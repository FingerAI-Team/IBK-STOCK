from src.config import DBConfig
import psycopg2 

class DBConnection:
    def __init__(self, config: DBConfig | None=None):
        self.config = config or DBConfig()
    
    def connect(self):
        self.conn = psycopg2.connect(
            host=self.config.host,
            dbname=self.config.db_name,
            user=self.config.user_id,
            password=self.config.user_pw,
            port=self.config.port
        )
        self.cur = self.conn.cursor()

    def close(self):
        self.cur.close()
        self.conn.close()