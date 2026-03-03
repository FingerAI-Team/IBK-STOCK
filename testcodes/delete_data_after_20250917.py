#!/usr/bin/env python3
"""
9월 17일 이후 데이터 삭제 스크립트
시간대 변환으로 인한 hash_value 불일치 문제 해결을 위해
"""
import os
import sys
import json
import logging
from datetime import datetime
from tqdm import tqdm

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.repositories.postgresql import DBConnection, PostgresDB

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('delete_data_after_20250917.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def delete_data_after_20250917(preview_mode=True):
    """
    2025년 9월 17일 이후의 모든 데이터를 삭제
    
    Args:
        preview_mode (bool): True면 미리보기만, False면 실제 삭제
    """
    
    # 데이터베이스 연결
    try:
        # DB 설정 파일 로드
        config_path = './config/'
        with open(os.path.join(config_path, 'db_config.json')) as f:
            db_config = json.load(f)
        
        db_conn = DBConnection(db_config)
        db_conn.connect()
        postgres = PostgresDB(db_conn)
        table_name = 'ibk_convlog'
        
        logger.info("🔗 데이터베이스 연결 성공")
        
        # 삭제할 데이터 조회
        logger.info("🔍 2025-09-17 이후 데이터 조회 중...")
        
        # conv_id가 20250917 이후인 데이터 조회
        postgres.db_connection.cur.execute(
            f"""
            SELECT conv_id, date, user_id, content 
            FROM {table_name} 
            WHERE conv_id >= '20250917_00000'
            ORDER BY conv_id
            """
        )
        
        records_to_delete = postgres.db_connection.cur.fetchall()
        
        if not records_to_delete:
            logger.info("✅ 삭제할 데이터가 없습니다.")
            return
        
        logger.info(f"📊 삭제 대상 데이터: {len(records_to_delete)}개")
        
        # 삭제할 데이터 미리보기
        logger.info("📋 삭제할 데이터 샘플 (처음 5개):")
        for i, record in enumerate(records_to_delete[:5]):
            conv_id, date, user_id, content = record
            logger.info(f"   {i+1}. {conv_id} | {date} | {user_id} | {content[:50]}...")
        
        if len(records_to_delete) > 5:
            logger.info(f"   ... 외 {len(records_to_delete) - 5}개 더")
        
        if preview_mode:
            logger.info("🔍 미리보기 모드 - 실제 삭제는 수행되지 않습니다.")
            logger.info("실제 삭제를 원하면 --execute 옵션을 사용하세요.")
            return
        
        # 사용자 확인
        print(f"\n⚠️  경고: {len(records_to_delete)}개의 데이터를 삭제하려고 합니다.")
        print("이 작업은 되돌릴 수 없습니다!")
        
        while True:
            confirm = input("정말로 삭제하시겠습니까? (yes/no): ").lower().strip()
            if confirm in ['yes', 'y']:
                break
            elif confirm in ['no', 'n']:
                logger.info("❌ 삭제 작업이 취소되었습니다.")
                return
            else:
                print("'yes' 또는 'no'를 입력해주세요.")
        
        # 실제 삭제 수행
        logger.info("🗑️  데이터 삭제 시작...")
        
        # 배치 삭제 (성능 향상)
        batch_size = 1000
        total_deleted = 0
        
        for i in tqdm(range(0, len(records_to_delete), batch_size), desc="삭제 진행"):
            batch = records_to_delete[i:i + batch_size]
            conv_ids = [record[0] for record in batch]
            
            # IN 절을 사용한 배치 삭제
            placeholders = ','.join(['%s'] * len(conv_ids))
            delete_query = f"DELETE FROM {table_name} WHERE conv_id IN ({placeholders})"
            
            postgres.db_connection.cur.execute(delete_query, conv_ids)
            total_deleted += len(conv_ids)
        
        # 트랜잭션 커밋
        postgres.db_connection.conn.commit()
        
        logger.info(f"✅ 삭제 완료: {total_deleted}개 데이터 삭제됨")
        
        # 삭제 후 통계
        postgres.db_connection.cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        remaining_count = postgres.db_connection.cur.fetchone()[0]
        logger.info(f"📊 남은 데이터: {remaining_count}개")
        
    except Exception as e:
        logger.error(f"❌ 오류 발생: {e}")
        if 'postgres' in locals():
            postgres.db_connection.conn.rollback()
        raise
    finally:
        if 'db_conn' in locals():
            db_conn.close()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='2025-09-17 이후 데이터 삭제')
    parser.add_argument('--execute', action='store_true', 
                       help='실제 삭제 수행 (기본값: 미리보기만)')
    
    args = parser.parse_args()
    
    logger.info("🚀 2025-09-17 이후 데이터 삭제 스크립트 시작")
    logger.info(f"모드: {'실제 삭제' if args.execute else '미리보기'}")
    
    try:
        delete_data_after_20250917(preview_mode=not args.execute)
        logger.info("✅ 스크립트 완료")
    except Exception as e:
        logger.error(f"❌ 스크립트 실패: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
