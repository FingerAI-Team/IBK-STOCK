from apscheduler.schedulers.blocking import BlockingScheduler
from src.services.ibk_service import IBKPipeline
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

def run_pipeline():
    pipeline = IBKPipeline()
    pipeline.run()

def start_scheduler():
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_pipeline,
        trigger="cron",
        minute="*/5",      # 5분마다 실행
        max_instances=1,   # 중복 실행 방지
        coalesce=True
    )
    logger.info("IBK Scheduler started")
    scheduler.start()


if __name__ == "__main__":
    start_scheduler()