from src.pipes.collect_pipe import DataCollector
from src.pipes.store_pipe import StorePipe
from src.envs.collect_api import IBKAPIEnv
from src.repositories.core import DBConnection
from src.core.settings import ONELINE_CONFIG, DB_CONFIG

ibk_api_env = IBKAPIEnv(config=ONELINE_CONFIG)
db_connection = DBConnection(config=DB_CONFIG)
data_collector = DataCollector(ibk_api_env)
store_pipe = StorePipe(db_connection=db_connection.conn)  # DB 연결은 실제 환경에 맞게 설정 필요

if __name__ == "__main__":
    day_logs = data_collector.run()
    print(f'type of day_logs: {type(day_logs)}')
    print(f'number of logs collected: {len(day_logs)}')
    print(f'sample log: {day_logs[0] if len(day_logs) > 0 else "No logs collected"}')