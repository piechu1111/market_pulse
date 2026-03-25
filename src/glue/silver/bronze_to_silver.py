import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window
from awsglue.job import Job


# helper functions
def is_valid_year_month(value: str) -> bool:
    """Checks if value is a valid YYYY-MM date."""
    if not isinstance(value, str):
        return False

    try:
        datetime.strptime(value.strip(), "%Y-%m")
        return True
    except ValueError:
        return False


def month_range(start_year_month: str, end_year_month: str):
    """
    Yields YYYY-MM strings from start_year_month to end_year_month (inclusive).
    Assumes both are valid YYYY-MM and start <= end.
    """
    start_year, start_month = map(int, start_year_month.split("-"))
    end_year, end_month = map(int, end_year_month.split("-"))

    year, month = start_year, start_month

    while (year < end_year) or (year == end_year and month <= end_month):
        yield f"{year:04d}-{month:02d}"
        month += 1
        if month > 12:
            month = 1
            year += 1


def validate_args(args: dict) -> None:
    """
    Validate job arguments and fail fast with clear messages.
    YEAR_MONTH_START and YEAR_MONTH_END are mandatory.
    """

    bronze_base = args.get("BRONZE_PATH", "").strip()
    silver_base = args.get("SILVER_PATH", "").strip()

    if not bronze_base:
        raise ValueError("BRONZE_PATH cannot be empty.")
    if not silver_base:
        raise ValueError("SILVER_PATH cannot be empty.")

    ym_start = args.get("YEAR_MONTH_START", "").strip()
    ym_end = args.get("YEAR_MONTH_END", "").strip()

    if not ym_start or not ym_end:
        raise ValueError(
            "YEAR_MONTH_START and YEAR_MONTH_END are mandatory and cannot be empty."
        )

    if not (is_valid_year_month(ym_start) and is_valid_year_month(ym_end)):
        raise ValueError(
            "YEAR_MONTH_START and YEAR_MONTH_END must have YYYY-MM format (e.g. 2024-01)."
        )

    if ym_end < ym_start:
        raise ValueError(
            f"YEAR_MONTH_END ({ym_end}) is earlier than YEAR_MONTH_START ({ym_start})."
        )

    # normalized values back to args (defensive, but explicit)
    args["YEAR_MONTH_START"] = ym_start
    args["YEAR_MONTH_END"] = ym_end

def main():
# required job arguments
    required_args = [
        "JOB_NAME",
        "BRONZE_PATH",
        "SILVER_PATH",
        "YEAR_MONTH_START",
        "YEAR_MONTH_END",
    ]

    # Glue will fail immediately if any required arg is missing
    args = getResolvedOptions(sys.argv, required_args)

    # validate given values
    validate_args(args)

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    bronze_base = args["BRONZE_PATH"]   # path in s3 to bronze e.g. s3://market-pulse-data-eu-central-1/data/bronze/alpha_vantage/intraday_1min/
    silver_base = args["SILVER_PATH"]   # path in s3 to silver e.g. s3://market-pulse-data-eu-central-1/data/silver/alpha_vantage/intraday_1min/

    ym_start = args["YEAR_MONTH_START"]
    ym_end = args["YEAR_MONTH_END"]

    # build input paths
    months = list(month_range(ym_start, ym_end))
    input_paths = [
        f"{bronze_base}symbol=*/month={m}/raw.json"
        for m in months
    ]

    # read bronze JSON files
    df_raw = (
        spark.read
             .option("multiLine", "true")
             .json(input_paths)
             .withColumn("source_file", F.input_file_name())
    )

    # extract symbol and month from S3 path
    df_with_meta = (
        df_raw
        .withColumn(
            "symbol",
            F.regexp_extract("source_file", r"symbol=([^/]+)", 1)
        )
        .withColumn(
            "month_path",
            F.regexp_extract("source_file", r"month=([0-9]{4}-[0-9]{2})", 1)
        )
    )

    # explode Alpha Vantage "Time Series (1min)" map
    df_exploded = (
        df_with_meta
        .select(
            "symbol",
            "month_path",
            "source_file",
            F.map_entries(F.col("`Time Series (1min)`")).alias("ts_entries")
        )
        .withColumn("ts_entry", F.explode("ts_entries"))
        .select(
            "symbol",
            "month_path",
            "source_file",
            F.col("ts_entry.key").alias("ts_str"),
            F.col("ts_entry.value").alias("bar_map")
        )
    )

    # parse OHLCV values and timestamps
    df_bars_raw = (
        df_exploded
        .select(
            "symbol",
            "month_path",
            "source_file",
            F.to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss").alias("ts"),
            F.col("bar_map.`1. open`").cast("double").alias("open"),
            F.col("bar_map.`2. high`").cast("double").alias("high"),
            F.col("bar_map.`3. low`").cast("double").alias("low"),
            F.col("bar_map.`4. close`").cast("double").alias("close"),
            F.col("bar_map.`5. volume`").cast("bigint").alias("volume"),
        )
        .withColumn("date", F.to_date("ts"))
        .withColumn("year_month", F.date_format("ts", "yyyy-MM"))
        .withColumn("ingestion_ts", F.current_timestamp())
    )

    # deduplicate by (symbol, ts) keeping latest ingestion
    # useful epsecially for reruns
    window = Window.partitionBy("symbol", "ts").orderBy(F.col("ingestion_ts").desc())

    df_silver = (
        df_bars_raw
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    # write silver layer as Parquet with partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        df_silver.write
            .partitionBy("symbol", "year_month")
            .mode("overwrite")  # overwrites only written partitions
            .parquet(silver_base)
    )

    job.commit()


# entry point with error handling
if __name__ == "__main__":
    try:
        main()
    except ValueError as e:
        print(f"[ARG ERROR] {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL ERROR] {e}", file=sys.stderr)
        sys.exit(1)
