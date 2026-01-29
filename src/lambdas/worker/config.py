import os
import sys
from dotenv import load_dotenv
from logging_config import setup_logging

load_dotenv()
logger = setup_logging()

def get_env_var(name: str) -> str:
    value = os.getenv(name)
    if not value:
        logger.error(f"Missing required env var: {name}")
        sys.exit(1)
    return value

# env variables
S3_BRONZE_BUCKET = get_env_var("S3_BRONZE_BUCKET")
S3_BRONZE_PREFIX = get_env_var("S3_BRONZE_PREFIX")
ALPHA_API_KEY = get_env_var("ALPHA_API_KEY")
ALPHA_API_URL = get_env_var("ALPHA_API_URL")