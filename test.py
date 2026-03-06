from src.pipes.collect_pipe import DataCollector
from src.pipes.store_pipe import StorePipe
from src.pipes.classify_pipe import DataClassifier
from src.pipes.transform_pipe import TransformPipe
from src.envs.collect_api import IBKAPIEnv
from src.repositories.core import DBConnection
from src.core.settings import ONELINE_CONFIG, DB_CONFIG

ibk_api_env = IBKAPIEnv(config=ONELINE_CONFIG)
db_connection = DBConnection(config=DB_CONFIG)
db_connection.connect()
data_collector = DataCollector(ibk_api_env)
transform_pipe = TransformPipe()
cls_pipe = DataClassifier()  # 모델 경로 등 필요한 설정은 DataClassifier 내부에서 처리하도록 수정
store_pipe = StorePipe(db_connection=db_connection.conn)  # DB 연결은 실제 환경에 맞게 설정 필요


if __name__ == "__main__":
    day_logs = data_collector.run()
    print(f'type of day_logs: {type(day_logs)}')
    print(f'number of logs collected: {len(day_logs)}')
    print(f'sample log: {day_logs[0] if len(day_logs) > 0 else "No logs collected"}')
    
    records = transform_pipe.run(day_log=day_logs)
    cls_records = cls_pipe.run(records=records)
    store_pipe.run(conv_log=records, cls_data=cls_records)
    print('done')
    db_connection.close()