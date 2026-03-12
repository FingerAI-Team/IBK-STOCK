from apscheduler.schedulers.blocking import BlockingScheduler
from src.services.ibk_service import IBKPipeline
from datetime import datetime, timedelta
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

def run_once(start_date=None, end_date=None):
    pipeline = IBKPipeline()
    pipeline.run(start_date, end_date)

def run_schedule():
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_once,
        trigger="interval",
        minutes=10,      # 5분마다 실행
        max_instances=1,
        # coalesce=True
    )
    logger.info("Scheduler started")
    scheduler.start()

def run_backfill(start_date):
    pipeline = IBKPipeline()
    start = datetime.strptime(start_date, "%Y-%m-%d")
    today = datetime.now()
    current = start
    while current.date() <= today.date():
        day = current.strftime("%Y-%m-%d")
        logger.info(f"Running backfill for {day}")
        pipeline.run(day, None)
        current += timedelta(days=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["once", "schedule", "backfill"],
        default="once",
        help="pipeline execution mode"
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default=None,
        help="start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help="end date (YYYY-MM-DD)"
    )
    args = parser.parse_args()
    if args.mode == "once":
        logger.info("Running pipeline once")
        run_once(args.start_date, args.end_date)

    elif args.mode == "schedule":
        logger.info("Running scheduler")
        run_schedule()

    elif args.mode == "backfill":
        if not args.start_date:
            raise ValueError("start_date is required for backfill")
        logger.info("Running backfill")
        run_backfill(args.start_date)