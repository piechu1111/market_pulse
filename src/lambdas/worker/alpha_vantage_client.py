import datetime
import json
import random
from time import sleep
from typing import Any, Dict, Optional
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
import atexit

from config import ALPHA_API_KEY, ALPHA_API_URL, S3_BRONZE_BUCKET, S3_BRONZE_PREFIX, logger
from s3_client import upload_json_to_s3, S3Config


# HTTP Session (no built-in backoff, manual retry below)
def _build_session() -> requests.Session:
    """
    Build a simple persistent requests.Session with connection pooling.
    Retry logic is implemented manually.
    """
    adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"Accept": "application/json"})
    return session


_HTTP_SESSION = _build_session()

atexit.register(_HTTP_SESSION.close)

REQUEST_TIMEOUT = (5, 15)  # (connect_timeout, read_timeout)
RATE_LIMIT_SLEEP_SECONDS = 0.1  # api limit is 75 requests/minute, 0.1 safety margin
JITTER_SECONDS = 0.9  # small jitter to avoid bursty patterns

RETRIES = 3  # additional retry attempts
RETRIABLE_STATUS = {429, 500, 502, 503, 504}


def _request_with_retries(
    sess: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[dict] = None,
    timeout: tuple = REQUEST_TIMEOUT,
) -> requests.Response:
    """
    Perform an HTTP request with retries.
    Retries apply to connection errors and retriable status codes:
    429, 500, 502, 503, 504.
    """
    attempts = 1 + RETRIES
    last_exception: Optional[Exception] = None
    response: Optional[requests.Response] = None

    for attempt in range(1, attempts + 1):
        try:
            response = sess.request(method, url, params=params, timeout=timeout)
        except requests.RequestException as e:
            last_exception = e
            logger.warning(
                "HTTP error on attempt %s/%s: %s %s | error=%s",
                attempt,
                attempts,
                method,
                url,
                e,
            )
            if attempt < attempts:
                sleep(RATE_LIMIT_SLEEP_SECONDS + random.uniform(0, JITTER_SECONDS))
                continue
            raise  # reraise if no retries left

        # success case
        if 200 <= response.status_code < 300:
            return response

        # retryable status codes
        if response.status_code in RETRIABLE_STATUS and attempt < attempts:
            logger.warning(
                "Retryable status %s on attempt %s/%s: %s %s | body[:200]=%r",
                response.status_code,
                attempt,
                attempts,
                method,
                url,
                response.text[:200],
            )
            sleep(RATE_LIMIT_SLEEP_SECONDS + random.uniform(0, JITTER_SECONDS))
            continue

        # non retryable or out of attempts
        return response

    # this should never happen, but ensures function always returns/raises
    if last_exception:
        raise last_exception
    assert response is not None
    return response

def bronze_local_path(symbol: str, month: str) -> Path:
    return (
        Path("data")
        / "bronze"
        / "alpha_vantage"
        / "intraday_1min"
        / f"symbol={symbol}"
        / f"month={month}"
        / "raw.json"
    )

def save_bronze_raw(
    data: Dict[str, Any],
    symbol: str,
    month: str,
    *,
    save_local: bool = True,
    s3_cfg: Optional["S3Config"] = None,
) -> Dict[str, Any]:
    """
    Saves raw API response (bronze) locally and/or to S3 
    and makes partitions by: symbol and month (eg. '2000-01').
    Returns dict with paths/URIs and status.
    If s3_cfg is not None then saves to S3.
    """
    result = {
        "ok": True,
        "local_path": None,
        "s3_uri": None,
        "error": None,
    }

    if save_local:
        try:
            local_path = bronze_local_path(symbol, month)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with local_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)

            result["local_path"] = local_path
            logger.info("Saved locally: %s", local_path)

        except Exception as e:
            logger.error("Local save failed: %s", e)
            result["ok"] = False
            result["error"] = f"local save failed: {e}"

    # S3 save if s3_cfg is not None
    if s3_cfg is not None:
        try:
            uri = upload_json_to_s3(data, symbol, month, s3_cfg)
            result["s3_uri"] = uri
        except Exception as e:
            logger.error("S3 save failed: %s", e)
            result["ok"] = False
            if result["error"]:
                result["error"] += f" | s3 save failed: {e}"
            else:
                result["error"] = f"s3 save failed: {e}"

    return result


def get_symbol_monthly_data(
    symbol: str = "IBM", 
    month: str = "2000-01",
    save: bool = False,
    session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """
    Fetch intraday data from the Alpha Vantage API for a given stock symbol and month.
    Optionally save to file system.

    Returns: {"ok": bool, "data": dict, "error": str (optional)}
    """
    sess = session or _HTTP_SESSION

    params = {
        "apikey": ALPHA_API_KEY,
        "function": "TIME_SERIES_INTRADAY",
        "interval": "1min",
        "outputsize": "full",
        "month": month,
        "symbol": symbol,
    }

    try:
        response = _request_with_retries(
            sess, "GET", ALPHA_API_URL, params=params, timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException as e:
        logger.error("Request to Alpha Vantage API failed after retries (symbol=%s, month=%s): %s", symbol, month, e)
        return {"ok": False, "error": "Failed to fetch data from Alpha Vantage API."}

    # non 2xx response after retries
    if not (200 <= response.status_code < 300):
        logger.error(
            "Alpha Vantage API returned non-2xx (symbol=%s, month=%s): %s %s",
            symbol,
            month,
            response.status_code,
            response.text[:300],
        )
        return {"ok": False, "error": f"Alpha Vantage API returned HTTP {response.status_code}."}

    # Content-Type validation
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "application/json" not in content_type:
        logger.error("Unexpected response type (symbol=%s, month=%s): %s", symbol, month, content_type)
        return {"ok": False, "error": "Unexpected response format."}

    # parse JSON safely
    try:
        data = response.json()
    except ValueError as e:
        logger.error(
            "Invalid JSON from Alpha Vantage API (symbol=%s, month=%s): %s; body[:300]=%r",
            symbol,
            month,
            e,
            response.text[:300],
        )
        return {"ok": False, "error": "Invalid JSON from Alpha Vantage API."}
    
    # Alpha Vantage likes to return "Note" on rate limit
    if "Note" in data:
        logger.warning("Alpha Vantage NOTE (rate limit?) for (symbol=%s, month=%s): %s", 
            symbol,
            month,
            data["Note"]
        )
        return {"ok": False, "error": data["Note"]}

    if "Error Message" in data:
        logger.error(
            "Alpha Vantage returned error for (symbol=%s, month=%s): %s", 
            symbol,
            month,
            data["Error Message"]
        )
        return {"ok": False, "error": data["Error Message"]}

    # save bronze raw if requested
    if save:
        s3_cfg = S3Config(bucket=S3_BRONZE_BUCKET, prefix=S3_BRONZE_PREFIX)
        raw_path = save_bronze_raw(data, symbol, month, save_local=False, s3_cfg=s3_cfg)
        logger.info("Saved bronze raw JSON: %s", raw_path)
    
    return {"ok": True, "data": data}


def symbol_search(
    keywords: str,
    save: bool = False,
    session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """
    Perform Alpha Vantage SYMBOL_SEARCH.
    Example: keywords='BA' -> Boeing, Bank of America, etc.
    Returns: {"ok": bool, "data": dict, "error": str (optional)}
    """
    sess = session or _HTTP_SESSION

    params = {
        "function": "SYMBOL_SEARCH",
        "keywords": keywords,
        "apikey": ALPHA_API_KEY,
    }

    try:
        response = _request_with_retries(sess, "GET", ALPHA_API_URL, params=params, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as e:
        logger.error("Symbol search request failed for keywords=%s: %s", keywords, e)
        return {"ok": False, "error": "Network error during symbol search."}

    if not (200 <= response.status_code < 300):
        logger.error(
            "Non-2xx response for symbol search (keywords=%s): %s %s",
            keywords,
            response.status_code,
            response.text[:300],
        )
        return {"ok": False, "error": f"HTTP {response.status_code}"}
    
    # Validate content type
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "application/json" not in content_type:
        logger.error(
            "Unexpected content type for symbol search (keywords=%s): %s",
            keywords,
            content_type,
        )
        return {"ok": False, "error": "Unexpected response format."}

    try:
        data = response.json()
    except ValueError as e:
        logger.error(
            "Invalid JSON in symbol search (keywords=%s): %s; body[:200]=%r",
            keywords,
            e,
            response.text[:200],
        )
        return {"ok": False, "error": "Invalid JSON from Alpha Vantage API."}

    # Alpha Vantage likes to return "Note" on rate limit
    if "Note" in data:
        logger.warning("Alpha Vantage NOTE (rate limit?) for keywords=%s: %s", keywords, data["Note"])
        return {"ok": False, "error": data["Note"]}

    if "Error Message" in data:
        logger.error("Alpha Vantage returned error for keywords=%s: %s", keywords, data["Error Message"])
        return {"ok": False, "error": data["Error Message"]}

    # optional save to bronze
    if save:
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat().replace(":", "-")
        file_path = (
            Path("data") / "bronze" / "alpha_vantage" / "symbol_search" / f"keywords={keywords}" / f"raw_{ts}.json"
        )
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        logger.info("Saved symbol search raw JSON: %s", file_path)

    return {"ok": True, "data": data}

def _fetch_monthly_adjusted(
    symbol: str,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    """
    common helper to fetch TIME_SERIES_MONTHLY_ADJUSTED.
    Returns: {"ok": bool, "data": dict, "error": str (optional)}
    """
    sess = session or _HTTP_SESSION

    params = {
        "apikey": ALPHA_API_KEY,
        "function": "TIME_SERIES_MONTHLY_ADJUSTED",
        "symbol": symbol,
    }

    try:
        response = _request_with_retries(
            sess, "GET", ALPHA_API_URL, params=params, timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException as e:
        logger.error("Request failed for monthly adjusted (symbol=%s): %s", symbol, e)
        return {"ok": False, "error": "Failed to fetch monthly adjusted data."}

    if not (200 <= response.status_code < 300):
        logger.error(
            "Non-2xx response for monthly adjusted (symbol=%s): %s %s",
            symbol,
            response.status_code,
            response.text[:300],
        )
        return {"ok": False, "error": f"HTTP {response.status_code}"}

    # Validate content type
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "application/json" not in content_type:
        logger.error("Unexpected content type for monthly adjusted (%s): %s", symbol, content_type)
        return {"ok": False, "error": "Unexpected response format."}

    try:
        data = response.json()
    except ValueError:
        logger.error(
            "Invalid JSON for monthly adjusted (%s). Body[:200]=%r",
            symbol,
            response.text[:200],
        )
        return {"ok": False, "error": "Invalid JSON from Alpha Vantage API."}

    # API internal error messages
    if "Error Message" in data:
        logger.error("Alpha Vantage error for %s: %s", symbol, data["Error Message"])
        return {"ok": False, "error": data["Error Message"]}

    if "Note" in data:
        logger.warning("Alpha Vantage Note (likely rate limit) for %s: %s", symbol, data["Note"])
        return {"ok": False, "error": data["Note"]}

    return {"ok": True, "data": data}

def get_symbol_monthly_adjusted_data(
    symbol: str = "IBM",
    save: bool = False,
    session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """
    Fetch monthly adjusted data from Alpha Vantage:
    function=TIME_SERIES_MONTHLY_ADJUSTED

    This is useful to detect if the symbol is valid / exists
    because Alpha Vantage returns an error message for invalid or delisted tickers.

    Returns: {"ok": bool, "data": dict, "error": str (optional)}
    """
    result = _fetch_monthly_adjusted(symbol, session=session)
    if not result.get("ok"):
        return result

    data = result["data"]

    # Save bronze if needed
    if save:
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat().replace(":", "-")
        file_path = (
            Path("data")
            / "bronze"
            / "alpha_vantage"
            / "monthly_adjusted"
            / f"symbol={symbol}"
            / f"raw_{ts}.json"
        )

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        logger.info("Saved monthly adjusted raw JSON: %s", file_path)

    return {"ok": True, "data": data}

def symbol_earliest_month(
    symbol: str,
    session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """
    Determine the earliest month available for a symbol using
    TIME_SERIES_MONTHLY_ADJUSTED.

    Returns:
        {
            "ok": True/False,
            "symbol": ...,
            "earliest_month": "YYYY-MM" or None,
            "error": optional str
        }
    """
    result = _fetch_monthly_adjusted(symbol, session=session)
    if not result.get("ok"):
        return {
            "ok": False,
            "symbol": symbol,
            "earliest_month": None,
            "error": result.get("error"),
        }

    data = result["data"]
    ts = data.get("Monthly Adjusted Time Series")
    if not ts:
        return {
            "ok": False,
            "symbol": symbol,
            "earliest_month": None,
            "error": "No time series",
        }

    # dates are keys like "2001-03-31"
    all_dates = list(ts.keys())
    all_dates.sort()  # earliest first

    earliest_full = all_dates[0]
    earliest_month = earliest_full[:7]  # convert to YYYY-MM

    return {
        "ok": True,
        "symbol": symbol,
        "earliest_month": earliest_month,
        "error": None,
    }
