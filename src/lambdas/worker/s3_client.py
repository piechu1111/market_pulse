import json
from pathlib import Path
import boto3
from typing import Optional, Dict, Any

from config import logger


class S3Config:
    """
    Configuration for AWS S3 access.
    Attributes:
        bucket (str): S3 bucket name.
        prefix (str): Prefix path within the bucket.
        client (boto3.client, optional): Pre-configured boto3 S3 client.
    """
    def __init__(self, bucket: str, prefix: str = "data", client: Optional["boto3.client"] = None):
        if not bucket:
            raise ValueError("S3 bucket cannot be empty")

        self.bucket = bucket
        self.prefix = prefix
        self.client = client

    def get_client(self) -> "boto3.client":
        # if client not provided create standard one
        return self.client or boto3.client("s3")


def make_s3_key(symbol: str, month: str, prefix: str) -> str:
    """
    Builds S3 key aligned with local structure.
    """
    return (
        Path(prefix)
        / "alpha_vantage"
        / "intraday_1min"
        / f"symbol={symbol}"
        / f"month={month}"
        / "raw.json"
    ).as_posix()


def upload_json_to_s3(data: Dict[str, Any], symbol: str, month: str, s3_cfg: S3Config) -> str:
    """
    Uploads JSON data to S3 using given S3 configuration,
    returns full URI (s3://bucket/key).
    """
    client = s3_cfg.get_client()
    key = make_s3_key(symbol, month, prefix=s3_cfg.prefix)
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    client.put_object(Bucket=s3_cfg.bucket, Key=key, Body=body)
    uri = f"s3://{s3_cfg.bucket}/{key}"
    logger.info("Saved JSON to S3: %s", uri)
    return uri
