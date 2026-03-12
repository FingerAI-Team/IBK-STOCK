from src.config import OnelineConfig
from src.config import DBConfig
from dotenv import load_dotenv
import os

load_dotenv()
ONELINE_CONFIG = OnelineConfig(
    bearer_token = os.getenv("BEARER_TOKEN", ""),
)

DB_CONFIG = DBConfig(
    host = os.getenv("DB_HOST", ""),
    db_name = os.getenv("DB_NAME", ""),
    user_id = os.getenv("DB_USER", ""),
    user_pw = os.getenv("DB_PASSWORD", ""),
    port = int(os.getenv("DB_PORT", 5432)),
)