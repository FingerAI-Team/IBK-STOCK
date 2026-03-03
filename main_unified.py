from src import UnifiedPipeline
from apscheduler.schedulers.blocking import BlockingScheduler
from scheduler_config import get_schedule_config
import argparse
import logging
import sys

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('main_unified.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_scheduled():
    """스케줄된 작업 실행"""
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--config_path', type=str, default='./config/')
    cli_parser.add_argument('--data_path', type=str, default='./data/')
    cli_parser.add_argument('--file_name', type=str, default='conv_log-0705-0902.xlsx')
    cli_parser.add_argument('--process', type=str, default='scheduled')
    cli_parser.add_argument('--task_name', type=str, default='cls')
    cli_parser.add_argument('--query', type=str, default=None)
    cli_args = cli_parser.parse_args()
    pipeline = UnifiedPipeline(cli_args)
    pipeline.run_full_pipeline()

if __name__ == '__main__':
    # 공통 argument parser 설정
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('--config_path', type=str, default='./config/')
    cli_parser.add_argument('--data_path', type=str, default='./data/')
    cli_parser.add_argument('--file_name', type=str, default='conv_log-0705-0902.xlsx')
    cli_parser.add_argument('--process', type=str, default='scheduled')
    cli_parser.add_argument('--task_name', type=str, default='cls')
    cli_parser.add_argument('--query', type=str, default=None)
    cli_parser.add_argument('--once', action='store_true', help='한 번만 실행 (오늘 날짜 기준 API 호출)')
    cli_args = cli_parser.parse_args()
    if cli_args.once:
        # 한 번만 실행 (오늘 날짜 기준 API 호출)
        cli_args.process = 'daily'  # API 호출을 위해 daily로 설정
        logger.info("🚀 일회성 실행 모드: 오늘 날짜 기준 API 데이터 수집 및 분석")
        pipeline = UnifiedPipeline(cli_args)
        pipeline.run_full_pipeline()
    else:
        # 스케줄러로 매 시간 실행
        scheduler = BlockingScheduler()
        schedule_config = get_schedule_config('hourly')  # 매시 5분에 실행
        scheduler.add_job(
            run_scheduled,
            trigger=schedule_config['trigger'],
            id='unified_pipeline_hourly',
            name=f"통합 파이프라인 {schedule_config['description']}",
            replace_existing=True
        )
        logger.info("통합 파이프라인이 시작됩니다:")
        logger.info("데이터 수집 + 🔍 데이터 분석: 매 정시 5분에 통합 실행")
        logger.info(f"스케줄: {schedule_config['description']}")
        logger.info("한 번만 실행하려면: python main_unified.py --once")
        try:
            scheduler.start()
        except KeyboardInterrupt:
            logger.info("⏹️ 스케줄러가 중단되었습니다")
            scheduler.shutdown()