from datetime import datetime
from time import sleep
from alpha_vantage_client import get_symbol_monthly_data, RATE_LIMIT_SLEEP_SECONDS
from config import logger
import time
import boto3
import io
import csv

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


def _read_symbols(symbols_path: str) -> list[dict]:
    """
    Reads CSV locally (dev) or from S3 (lambda).
    Returns a list of dicts.
    """
    if symbols_path.startswith("s3://"):
        b, k = _parse_s3_uri(symbols_path)
        obj = s3.get_object(Bucket=b, Key=k)
        body = obj["Body"].read().decode("utf-8")
        return list(csv.DictReader(io.StringIO(body)))

    with open(symbols_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fetch_and_store_alpha_vantage_data(
    symbols_path: str,
    start_year_month: str,
    end_year_month: str,
    limit=None,
    symbols_subset=None,
):
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
        "Worker started with range %s -> %s, symbols_subset size=%s",
        start_year_month,
        end_year_month,
        subset_size,
    )

    start_ts = time.time()
    rows = _read_symbols(symbols_path)

    if not rows:
        raise ValueError("Input CSV is empty")

    required_columns = {"symbol", "start_date"}
    missing = required_columns - set(rows[0].keys())
    if missing:
        raise ValueError(
            f"Input CSV must contain columns: {sorted(required_columns)}. Missing: {sorted(missing)}"
        )

    if symbols_subset is not None:
        allowed = set(symbols_subset)
        rows = [row for row in rows if row.get("symbol") in allowed]
        logger.info("Batch mode enabled: %s symbols in this batch", len(rows))

    if limit is not None:
        rows = rows[: int(limit)]
        logger.info("Limiting to first %s symbols", limit)

    today_year_month = datetime.utcnow().strftime("%Y-%m")
    max_end_year_month = min(end_year_month, today_year_month)

    total_symbols = len(rows)
    skipped_symbols = 0
    total_requests = 0
    success_requests = 0
    error_requests = 0
    symbols_with_errors = set()
    failed_pairs = set()

    logger.info(
        "Starting Alpha Vantage fetch for %s symbols, range: %s → %s",
        total_symbols,
        start_year_month,
        max_end_year_month,
    )

    for idx, row in enumerate(rows, start=1):
        symbol = (row.get("symbol") or "").strip()
        first_year_month = str(row.get("start_date") or "").strip()

        logger.info("Processing symbol %s/%s: %s", idx, total_symbols, symbol)

        if not symbol:
            logger.info("[SKIP] empty symbol")
            skipped_symbols += 1
            continue

        if not is_valid_year_month(first_year_month):
            logger.info("[SKIP] %s — invalid start_date %s", symbol, first_year_month)
            skipped_symbols += 1
            continue

        if first_year_month > max_end_year_month:
            logger.info(
                "[SKIP] %s — symbol starts at %s, later than requested end %s",
                symbol,
                first_year_month,
                max_end_year_month,
            )
            skipped_symbols += 1
            continue

        effective_start = max(start_year_month, first_year_month)
        effective_end = max_end_year_month

        for month in month_range(effective_start, effective_end):
            logger.info("[FETCH] %s — month %s", symbol, month)
            total_requests += 1

            response = get_symbol_monthly_data(symbol=symbol, month=month, save=True)

            if not response.get("ok"):
                error_requests += 1
                symbols_with_errors.add(symbol)
                failed_pairs.add((symbol, month))
                logger.error("[ERR] %s — %s for %s", symbol, response.get("error"), month)
                sleep(RATE_LIMIT_SLEEP_SECONDS)
                continue

            success_requests += 1
            failed_pairs.discard((symbol, month))
            logger.info("[OK] %s — stored for month %s", symbol, month)
            sleep(RATE_LIMIT_SLEEP_SECONDS)

    duration_seconds = time.time() - start_ts
    requests_per_minute = (
        total_requests / duration_seconds * 60 if duration_seconds > 0 else 0.0
    )
    unresolved_error_requests = len(failed_pairs)
    processed_symbols = total_symbols - skipped_symbols

    logger.info("Data fetching and storing completed.")
    logger.info("========== SUMMARY ==========")
    logger.info("Total symbols         : %s", total_symbols)
    logger.info("Processed symbols     : %s", processed_symbols)
    logger.info("Skipped symbols       : %s", skipped_symbols)
    logger.info("Total API requests    : %s", total_requests)
    logger.info("Successful requests   : %s", success_requests)
    logger.info("Error requests        : %s", error_requests)
    logger.info("Symbols with any error: %s", len(symbols_with_errors))
    logger.info("Unresolved errors     : %s", unresolved_error_requests)
    logger.info("Requests/minute       : %.2f", requests_per_minute)
    logger.info("Duration (sec)        : %.2f", duration_seconds)

    return {
        "ok": unresolved_error_requests == 0,
        "total_symbols": total_symbols,
        "processed_symbols": processed_symbols,
        "skipped_symbols": skipped_symbols,
        "total_requests": total_requests,
        "successful_requests": success_requests,
        "error_requests": error_requests,
        "symbols_with_errors": sorted(symbols_with_errors),
        "unresolved_error_requests": unresolved_error_requests,
        "failed_pairs": [
            {"symbol": symbol, "month": month}
            for symbol, month in sorted(failed_pairs)
        ],
        "requests_per_minute": requests_per_minute,
        "duration_seconds": duration_seconds,
    }