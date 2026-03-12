from apscheduler.schedulers.blocking import BlockingScheduler
from src.config import OnelineConfig
from src.core.container import build_container
from datetime import datetime, timedelta
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

def run_once(service, start_date=None, end_date=None):
    logger.info("Running pipeline once")
    service["ibk_service"].run(start_date, end_date)

def run_schedule(service):
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_once,
        trigger="interval",
        minutes=10,
        max_instances=1,
        args=[service]
    )
    logger.info("Scheduler started")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")

def run_backfill(service, start_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    today = datetime.now()
    current = start
    while current.date() <= today.date():
        day = current.strftime("%Y-%m-%d")
        logger.info(f"Running backfill for {day}")
        service["ibk_service"].run(day, None)
        current += timedelta(days=1)


def main(args):
    logger.info("Building container")
    service = build_container(oneline_config=OnelineConfig())
    logger.info("Container ready")
    try:
        if args.mode == "once":
            run_once(service, args.start_date, args.end_date)
        elif args.mode == "schedule":
            run_schedule(service)
        elif args.mode == "backfill":
            if not args.start_date:
                raise ValueError("start_date is required for backfill")
            run_backfill(service, args.start_date)
    finally:
        # 🔥 프로그램 종료 시 DB close
        if "db_connection" in service:
            service["db_connection"].close()
            logger.info("DB connection closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["once", "schedule", "backfill"],
        default="once"
    )
    parser.add_argument(
        "--start_date",
        type=str,
        default=None
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None
    )
    args = parser.parse_args()
    main(args)