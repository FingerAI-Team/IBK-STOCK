class HashRepository:
    def __init__(self, conn):
        self.conn = conn

    def find_all(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM ibk_hash")
            return cur.fetchall()
        
    def check_hash_duplicate(self, table_name, hash_id):
        '''
        테이블에 동일한 hash_id가 존재하는지 확인합니다. 
        이미 존재하는 해시인 경우, True를 반환합니다.        
        args:
          table_name (str): 테이블 이름
          hash_id (str): 해시 ID
        returns:
          bool: 중복 여부 (True: 중복됨, False: 중복되지 않음)
        '''
        self.db_connection.conn.commit()
        self.db_connection.cur.execute(f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE hash_id = %s)", (hash_id,))
        result = self.db_connection.cur.fetchone()
        return result[0] if result else False
       
    
    def get_or_create(self, hash_value: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT hash_id FROM ibk_hash WHERE hash_value = %s",
                (hash_value,)
            )
            row = cur.fetchone()
            if row:
                return row[0]
            cur.execute(
                "INSERT INTO ibk_hash (hash_value) VALUES (%s) RETURNING hash_id",
                (hash_value,)
            )
            return cur.fetchone()[0]