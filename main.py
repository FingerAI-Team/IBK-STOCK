from apscheduler.schedulers.blocking import BlockingScheduler
from src.services.ibk_service import IBKPipeline
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

def run_once():
    pipeline = IBKPipeline()
    pipeline.run()

def run_schedule():
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_once,
        trigger="cron",
        minute="*/5",      # 5분마다 실행
        max_instances=1,
        coalesce=True
    )
    logger.info("Scheduler started")
    scheduler.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["once", "schedule"],
        default="once",
        help="pipeline execution mode"
    )
    args = parser.parse_args()
    if args.mode == "once":
        logger.info("Running pipeline once")
        run_once()

    elif args.mode == "schedule":
        logger.info("Running scheduler")
        run_schedule()