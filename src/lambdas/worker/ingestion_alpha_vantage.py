from datetime import datetime
from time import sleep
from alpha_vantage_client import get_symbol_monthly_data, RATE_LIMIT_SLEEP_SECONDS
import pandas as pd
from config import logger
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import time

import boto3
import io

s3 = boto3.client("s3")


def is_valid_year_month(value: str) -> bool:
    """Checks if value is a valid YYYY-MM date."""
    if not isinstance(value, str):
        return False
    try:
        datetime.strptime(value.strip(), "%Y-%m")
        return True
    except Exception:
        return False


def month_range(start_year_month: str, end_year_month: str):
    start_year, start_month = map(int, start_year_month.split("-"))
    end_year, end_month = map(int, end_year_month.split("-"))

    year, month = start_year, start_month
    while (year < end_year) or (year == end_year and month <= end_month):
        yield f"{year:04d}-{month:02d}"
        month += 1
        if month > 12:
            month = 1
            year += 1


def _parse_s3_uri(uri: str):
    assert uri.startswith("s3://")
    no = uri[5:]
    bucket, key = no.split("/", 1)
    return bucket, key


def _read_symbols_df(symbols_path: str) -> pd.DataFrame:
    """
    Reads CSV locally (dev) or from S3 (lambda).
    """
    if symbols_path.startswith("s3://"):
        b, k = _parse_s3_uri(symbols_path)
        obj = s3.get_object(Bucket=b, Key=k)
        body = obj["Body"].read()
        return pd.read_csv(io.BytesIO(body))
    return pd.read_csv(symbols_path)


def fetch_and_store_alpha_vantage_data(
    symbols_path: str,
    start_year_month: str,
    end_year_month: str,
    limit=None,
    symbols_subset=None, # list of symbols for a batch (e.g. 50)
):
    # validate input range
    if not is_valid_year_month(start_year_month):
        raise ValueError(f"start_year_month is invalid: {start_year_month}")

    if not is_valid_year_month(end_year_month):
        raise ValueError(f"end_year_month is invalid: {end_year_month}")

    if end_year_month < start_year_month:
        raise ValueError(
            f"end_year_month {end_year_month} is earlier than start_year_month {start_year_month}"
        )
    
    subset_size = "ALL" if symbols_subset is None else len(symbols_subset)
    logger.info(
        f"Worker started with range {start_year_month} -> {end_year_month}, "
        f"symbols_subset size={subset_size}"
    )
    print(f"logger.level: {logger.level}")

    start_ts = time.time()

    df = _read_symbols_df(symbols_path)

    if "symbol" not in df.columns or "start_date" not in df.columns:
        raise ValueError("Input CSV must contain 'symbol' and 'start_date' columns")

    # batch mode – only symbols from this batch
    if symbols_subset is not None:
        df = df[df["symbol"].isin(symbols_subset)].reset_index(drop=True)
        logger.info(f"Batch mode enabled: {len(df)} symbols in this batch")

    if limit is not None:
        df = df.head(limit)
        logger.info(f"Limiting to first {limit} symbols: {list(df['symbol'])}")

    # do not fetch future months for safety (use UTC to be deterministic)
    today_year_month = datetime.utcnow().strftime("%Y-%m")
    max_end_year_month = min(end_year_month, today_year_month)

    # statistics for logging
    total_symbols = len(df)
    skipped_symbols = 0
    total_requests = 0
    success_requests = 0
    error_requests = 0
    symbols_with_errors = set()
    failed_pairs = set()

    logger.info(
        f"Starting Alpha Vantage fetch for {total_symbols} symbols, "
        f"range: {start_year_month} → {max_end_year_month}"
    )

    with logging_redirect_tqdm():
        for idx in tqdm(df.index, desc="Symbols", unit="symbol"):
            symbol = df.at[idx, "symbol"]
            first_year_month = str(df.at[idx, "start_date"]).strip()

            if not is_valid_year_month(first_year_month):
                logger.info(f"[SKIP] {symbol} — invalid start_date {first_year_month}")
                skipped_symbols += 1
                continue

            if first_year_month > max_end_year_month:
                logger.info(
                    f"[SKIP] {symbol} — symbol starts at {first_year_month}, "
                    f"later than requested end {max_end_year_month}"
                )
                skipped_symbols += 1
                continue

            effective_start = max(start_year_month, first_year_month)
            effective_end = max_end_year_month

            for month in month_range(effective_start, effective_end):
                logger.info(f"[FETCH] {symbol} — month {month}")
                total_requests += 1

                # overwrite in S3 (same key)
                response = get_symbol_monthly_data(symbol=symbol, month=month, save=True)

                if not response.get("ok"):
                    error_requests += 1
                    symbols_with_errors.add(symbol)
                    failed_pairs.add((symbol, month))
                    logger.error(f"[ERR] {symbol} — {response.get('error')} for {month}")
                    sleep(RATE_LIMIT_SLEEP_SECONDS)
                    continue

                success_requests += 1
                if (symbol, month) in failed_pairs:
                    failed_pairs.remove((symbol, month))
                logger.info(f"[OK] {symbol} — stored for month {month}")
                sleep(RATE_LIMIT_SLEEP_SECONDS)

    duration_seconds = time.time() - start_ts
    requests_per_minute = (
        total_requests / duration_seconds * 60 if duration_seconds > 0 else 0.0
    )
    unresolved_error_requests = len(failed_pairs)

    logger.info("Data fetching and storing completed.")
    processed_symbols = total_symbols - skipped_symbols
    logger.info("========== SUMMARY ==========")
    logger.info(f"Total symbols         : {total_symbols}")
    logger.info(f"Processed symbols     : {processed_symbols}")
    logger.info(f"Skipped symbols       : {skipped_symbols}")
    logger.info(f"Total API requests    : {total_requests}")
    logger.info(f"Successful requests   : {success_requests}")
    logger.info(f"Error requests        : {error_requests}")
    logger.info(f"Symbols with any error: {len(symbols_with_errors)}")
    logger.info(f"Unresolved errors     : {unresolved_error_requests}")
    logger.info(f"Total duration        : {duration_seconds:.1f} s")
    logger.info(f"Requests per minute   : {requests_per_minute:.2f}")

    if symbols_with_errors:
        logger.info(
            "Error symbols (any month failed): " + ", ".join(sorted(symbols_with_errors))
        )

    return {
        "ok": True,
        "message": "Data fetching and storing completed.",
        "stats": {
            "total_symbols": total_symbols,
            "processed_symbols": processed_symbols,
            "skipped_symbols": skipped_symbols,
            "total_requests": total_requests,
            "success_requests": success_requests,
            "error_requests": error_requests,
            "unresolved_error_requests": unresolved_error_requests,
            "duration_seconds": duration_seconds,
            "requests_per_minute": requests_per_minute,
            "symbols_with_errors": sorted(symbols_with_errors),
        },
    }