from src.pipes.collect_pipe import DataCollector
from src.pipes.store_pipe import StorePipe
from src.pipes.classify_pipe import DataClassifier
from src.pipes.transform_pipe import TransformPipe
from src.envs.collect_api import IBKAPIEnv
from src.repositories.core import DBConnection
from src.repositories.conv_repo import ConvRepository
from src.repositories.cls_repo import ClsRepository   
from src.core.settings import ONELINE_CONFIG, DB_CONFIG

ibk_api_env = IBKAPIEnv(config=ONELINE_CONFIG)
db_connection = DBConnection(config=DB_CONFIG)
db_connection.connect()
data_collector = DataCollector(ibk_api_env)

conv_repo = ConvRepository(db_connection.conn)
cls_repo = ClsRepository(db_connection.conn)

transform_pipe = TransformPipe(conv_repo=conv_repo)
cls_pipe = DataClassifier()
store_pipe = StorePipe(
    conv_repo=conv_repo,
    cls_repo=cls_repo,
)

if __name__ == "__main__":
    day_logs = data_collector.run()
    print(f'type of day_logs: {type(day_logs)}')
    print(f'number of logs collected: {len(day_logs)}')
    print(f'sample log: {day_logs[0] if len(day_logs) > 0 else "No logs collected"}')
    
    records = transform_pipe.run(day_log=day_logs)
    cls_records = cls_pipe.run(records=records)
    store_pipe.run(conv_records=records, cls_data=cls_records)
    print('done')
    db_connection.close()