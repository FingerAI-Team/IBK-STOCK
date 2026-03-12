from src.repositories.conv_repo import ConvRepository
from src.repositories.cls_repo import ClsRepository
from src.pipes.transform_pipe import TransformPipe
from src.pipes.classify_pipe import DataClassifier
from src.pipes.collect_pipe import DataCollector
from src.pipes.store_pipe import StorePipe
from src.envs.collect_api import IBKAPIEnv
import logging

logger = logging.getLogger(__name__)


class IBKPipeline:
    def __init__(self, api_env: IBKAPIEnv, conv_repo: ConvRepository, cls_repo: ClsRepository, 
            collect_pipe: DataCollector, transform_pipe: TransformPipe, classify_pipe: DataClassifier, store_pipe: StorePipe):
        self.api_env = api_env
        self.conv_repo = conv_repo
        self.cls_repo = cls_repo
        self.collect_pipe = collect_pipe
        self.transform_pipe = transform_pipe
        self.classify_pipe = classify_pipe
        self.store_pipe = store_pipe

    def run(self, start_date=None, end_date=None):
        logger.info("pipeline start")
        try:
            day_logs = self.collect_pipe.run(start_date, end_date)
            logger.info(f"logs collected: {len(day_logs)}")
            records = self.transform_pipe.run(day_log=day_logs)
            logger.info(f"records transformed: {len(records)}")
            cls_records = self.classify_pipe.run(records)
            logger.info(f"classification rows: {len(cls_records)}")
            self.store_pipe.run(
                conv_records=records,
                cls_data=cls_records
            )
            logger.info("pipeline finished")
        except Exception as e:
            logger.exception(f"pipeline error: {e}")
        finally:
            self.db_connection.close()