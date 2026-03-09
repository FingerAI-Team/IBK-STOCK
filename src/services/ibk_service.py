from src.repositories.conv_repo import ConvRepository
from src.repositories.cls_repo import ClsRepository
from src.repositories.core import DBConnection
from src.pipes.transform_pipe import TransformPipe
from src.pipes.classify_pipe import DataClassifier
from src.pipes.collect_pipe import DataCollector
from src.pipes.store_pipe import StorePipe
from src.envs.collect_api import IBKAPIEnv
from src.core.settings import ONELINE_CONFIG, DB_CONFIG
import logging

logger = logging.getLogger(__name__)


class IBKPipeline:
    def __init__(self):
        self.api_env = IBKAPIEnv(config=ONELINE_CONFIG)
        self.db_connection = DBConnection(config=DB_CONFIG)

        self.conv_repo = None
        self.cls_repo = None

        self.collect_pipe = None
        self.transform_pipe = None
        self.classify_pipe = None
        self.store_pipe = None

    def initialize(self):
        self.db_connection.connect()
        self.conv_repo = ConvRepository(self.db_connection.conn)
        self.cls_repo = ClsRepository(self.db_connection.conn)
        self.collect_pipe = DataCollector(self.api_env)
        self.transform_pipe = TransformPipe(
            conv_repo=self.conv_repo
        )
        self.classify_pipe = DataClassifier()
        self.store_pipe = StorePipe(
            conv_repo=self.conv_repo,
            cls_repo=self.cls_repo
        )

    def run(self):
        logger.info("pipeline start")
        try:
            self.initialize()
            day_logs = self.collect_pipe.run()
            logger.info(f"logs collected: {len(day_logs)}")
            if not day_logs:
                return

            records = self.transform_pipe.run(day_log=day_logs)
            logger.info(f"records transformed: {len(records)}")
            if not records:
                return

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