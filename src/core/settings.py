from src.config import OnelineConfig
from dotenv import load_dotenv
import os

load_dotenv()
ONELINE_CONFIG = OnelineConfig(
    bearer_token = os.getenv("BEARER_TOKEN", ""),
)
