import boto3, csv, io
from datetime import datetime, timezone

s3 = boto3.client("s3")

def parse_s3_uri(uri: str):
    """
    Parse an S3 URI (s3://bucket/key...) into (bucket, key).
    """
    assert uri.startswith("s3://")
    b, k = uri[5:].split("/", 1)
    return b, k

def chunk(lst, n):
    """
    Yield successive n-sized chunks from a list.
    """
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def _is_valid_year_month(value: str) -> bool:
    """
    Validate YYYY-MM format (e.g. '2025-12').
    """
    if not isinstance(value, str):
        return False
    try:
        datetime.strptime(value.strip(), "%Y-%m")
        return True
    except ValueError:
        return False

def _parse_event_time_utc(event: dict) -> datetime:
    """
    EventBridge / Scheduler often provides event['time'] in ISO format,
    e.g. '2025-12-17T10:00:00Z'.
    If missing, fall back to the current UTC time.
    """
    t = event.get("time")
    if isinstance(t, str) and t.strip():
        # handle ISO format with trailing 'Z'
        return datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(timezone.utc)
    return datetime.now(timezone.utc)

def _prev_month(year: int, month: int) -> tuple[int, int]:
    """
    Return (year, month) for the previous calendar month.
    """
    if month == 1:
        return (year - 1, 12)
    return (year, month - 1)

def handler(event, context):
    symbols_s3_uri = event["symbols_s3_uri"]
    batch_size = int(event.get("batch_size", 50))

    # "frozen" scheduling time based on EventBridge event, if available
    now = _parse_event_time_utc(event)

    # input = ingest range (explicit, for backfill and idempotency)
    year_month_start = (event.get("year_month_start") or "").strip()
    year_month_end = (event.get("year_month_end") or "").strip()

    # if the scheduler did not provide a range -> compute it automatically
    if not year_month_start or not year_month_end:
        # default: previous month + current month (to capture transitions)
        py, pm = _prev_month(now.year, now.month)
        year_month_start = f"{py:04d}-{pm:02d}"
        year_month_end = f"{now.year:04d}-{now.month:02d}"

    if not (_is_valid_year_month(year_month_start) and _is_valid_year_month(year_month_end)):
        raise ValueError("year_month_start and year_month_end must be in YYYY-MM format (e.g. 2025-12).")

    if year_month_end < year_month_start:
        raise ValueError(f"year_month_end ({year_month_end}) is earlier than year_month_start ({year_month_start}).")

    # computed ingest range (currently equal to input, but explicitly named)
    ingest_year_month_start = year_month_start
    ingest_year_month_end = year_month_end

    # computed Glue range: full previous calendar year + ingest_end
    end_year = int(ingest_year_month_end[:4])
    glue_year_month_start = f"{end_year - 1}-01"
    glue_year_month_end = ingest_year_month_end

    # safety check for yearly silver partitions
    if glue_year_month_start[5:7] != "01":
        raise ValueError("Internal error: glue_year_month_start must be January (YYYY-01).")

    # Glue range must fully cover the ingest range
    if glue_year_month_start > ingest_year_month_start or glue_year_month_end < ingest_year_month_end:
        raise ValueError("Glue range must fully cover ingest range.")

    # read symbols
    b, k = parse_s3_uri(symbols_s3_uri)
    obj = s3.get_object(Bucket=b, Key=k)
    text = obj["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(text))
    symbols = [row["symbol"].strip() for row in reader]

    batches = []
    for batch_id, sym_batch in enumerate(chunk(symbols, batch_size)):
        batches.append({
            "batch_id": batch_id,
            "symbols_subset": sym_batch,
            "symbols_s3_uri": symbols_s3_uri,

            # worker uses ingest-range
            "year_month_start": ingest_year_month_start,
            "year_month_end": ingest_year_month_end,
        })

    return {
        "batches": batches,
        "total_symbols": len(symbols),

        # for Glue silver
        "glue_year_month_start": glue_year_month_start,
        "glue_year_month_end": glue_year_month_end,
        # for gold
        "gold_year_start": glue_year_month_start[:4],
        "gold_year_end": glue_year_month_end[:4],

        # debug info
        "ingest_year_month_start": ingest_year_month_start,
        "ingest_year_month_end": ingest_year_month_end,
        "planned_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }