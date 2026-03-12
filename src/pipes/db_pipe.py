from src.repositories.core import DBConnection
from src.repositories.conv_repo import ConvRepository
from src.repositories.cls_repo import ClsRepository
from src.config import DBConfig

class DBPipe:
    def __init__(self, db_config: DBConfig | None = None):
        self.db_config = db_config or DBConfig()
        self.db_conn = None

    def _connect(self):
        self.db_conn = DBConnection(self.db_config)
        self.db_conn.connect()

    def _close(self):
        if self.db_conn:
            self.db_conn.close()

    def _get_conv_data(self):
        conv_repo = ConvRepository(self.db_conn.conn)
        return conv_repo.find_all()

    def _get_cls_data(self):
        cls_repo = ClsRepository(self.db_conn.conn)
        return cls_repo.find_all()
    
    def run(self, data_type: str):
        self._connect()
        try:
            if data_type == 'conv':
                return self._get_conv_data()
            elif data_type == 'cls':
                return self._get_cls_data()
            elif data_type == 'all':
                return {
                    'conv': self._get_conv_data(),
                    'cls': self._get_cls_data()
                }
            else:
                raise ValueError(f"Unsupported data type: {data_type}")
        finally:
            self._close()