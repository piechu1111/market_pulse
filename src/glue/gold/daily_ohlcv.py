import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window
from awsglue.job import Job


# helpers
def is_valid_year(value: str) -> bool:
    """Checks if value is a valid YYYY year."""
    if not isinstance(value, str):
        return False
    try:
        datetime.strptime(value.strip(), "%Y")
        return True
    except ValueError:
        return False


def validate_args(args: dict) -> None:
    """
    Validate job arguments and fail fast.
    YEAR_START i YEAR_END są obowiązkowe.
    """
    silver_base = args.get("SILVER_PATH", "").strip()
    gold_base = args.get("GOLD_DAILY_PATH", "").strip()

    if not silver_base:
        raise ValueError("SILVER_PATH cannot be empty.")
    if not gold_base:
        raise ValueError("GOLD_DAILY_PATH cannot be empty.")

    year_start = args.get("YEAR_START", "").strip()
    year_end = args.get("YEAR_END", "").strip()

    if not year_start or not year_end:
        raise ValueError("YEAR_START and YEAR_END are mandatory and cannot be empty.")

    if not (is_valid_year(year_start) and is_valid_year(year_end)):
        raise ValueError("YEAR_START and YEAR_END must have YYYY format (e.g. 2010).")

    if int(year_end) < int(year_start):
        raise ValueError(
            f"YEAR_END ({year_end}) is earlier than YEAR_START ({year_start})."
        )

    args["YEAR_START"] = year_start
    args["YEAR_END"] = year_end


# main
def main():
    required_args = [
        "JOB_NAME",
        "SILVER_PATH",
        "GOLD_DAILY_PATH",
        "YEAR_START",
        "YEAR_END",
    ]

    args = getResolvedOptions(sys.argv, required_args)
    validate_args(args)

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    silver_base = args["SILVER_PATH"]
    gold_base = args["GOLD_DAILY_PATH"]

    year_start = int(args["YEAR_START"])
    year_end = int(args["YEAR_END"])
    years_span = year_end - year_start + 1

    # heuristic mapping shuffle partitions
    if years_span <= 1:
        shuffle_parts = 64
    elif years_span <= 5:
        shuffle_parts = 128
    elif years_span <= 10:
        shuffle_parts = 200  # default
    else:
        shuffle_parts = 256

    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_parts))
    print(
        f"Using spark.sql.shuffle.partitions = {shuffle_parts} for years_span={years_span}"
    )

    # read Silver (1-min OHLCV)
    # assumption: silver table has columns: symbol, ts, date, year, open, high, low, close, volume
    # and is partitioned by (symbol, year).
    df_silver = (
        spark.read.parquet(silver_base)
        .filter((F.col("year") >= year_start) & (F.col("year") <= year_end))
    )

    # just in case, drop rows with null timestamp
    df_silver = df_silver.filter(F.col("ts").isNotNull())

    # making sure we have date column (if silver doesn't have it, uncomment the line below)
    # df_silver = df_silver.withColumn("date", F.to_date("ts"))

    # aggregation to daily OHLCV
    # open = first open same day
    # high = max high
    # low  = min low
    # close = last close same day
    # volume = sum volume
    daily_agg = (
        df_silver.groupBy("symbol", "date")
        .agg(
            F.first("open").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.last("close").alias("close"),
            F.sum("volume").alias("volume"),
        )
        .withColumn("year", F.year("date"))
    )

    # daily_return = (close_today / close_yesterday) - 1
    w_sym = Window.partitionBy("symbol").orderBy("date")

    df_daily = (
        daily_agg
        .withColumn("prev_close", F.lag("close").over(w_sym))
        .withColumn(
            "return_1d",
            F.when(F.col("prev_close").isNotNull(),
                   F.col("close") / F.col("prev_close") - F.lit(1.0)
            ).otherwise(F.lit(None).cast("double")),
        )
        .drop("prev_close")
    )

    # it is good to physically group by year, as we partition by year
    df_daily = df_daily.repartition("year")


    # write GOLD: daily_ohlcv
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    (
        df_daily.write
        .partitionBy("year") # layout: .../gold/daily_ohlcv/year=2020/part-*.parquet
        .mode("overwrite") 
        .parquet(gold_base)
    )

    job.commit()


if __name__ == "__main__":
    try:
        main()
    except ValueError as e:
        print(f"[ARG ERROR] {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL ERROR] {e}", file=sys.stderr)
        sys.exit(1)