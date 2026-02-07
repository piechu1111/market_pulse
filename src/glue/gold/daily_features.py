import sys
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

from pyspark.sql import functions as F, Window



# helpers
def is_valid_year(value: str) -> bool:
    """Checks if the value is a valid YYYY year."""
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
    YEAR_START and YEAR_END are mandatory.
    """

    daily_base = args.get("DAILY_PATH", "").strip()
    features_base = args.get("FEATURES_PATH", "").strip()

    if not daily_base:
        raise ValueError("DAILY_PATH cannot be empty.")
    if not features_base:
        raise ValueError("FEATURES_PATH cannot be empty.")

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
        "DAILY_PATH",
        "FEATURES_PATH",
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

    daily_base = args["DAILY_PATH"]
    features_base = args["FEATURES_PATH"]

    year_start = int(args["YEAR_START"])
    year_end = int(args["YEAR_END"])
    years_span = year_end - year_start + 1

    # heuristic mapping for shuffle partitions
    if years_span <= 1:
        shuffle_parts = 64
    elif years_span <= 5:
        shuffle_parts = 128
    elif years_span <= 10:
        shuffle_parts = 200
    else:
        shuffle_parts = 256

    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_parts))
    print(
        f"Using spark.sql.shuffle.partitions = {shuffle_parts} for years_span={years_span}"
    )

    # read GOLD daily_ohlcv
    df_daily = (
        spark.read.parquet(daily_base)
        .filter((F.col("year") >= year_start) & (F.col("year") <= year_end))
        .select("symbol", "date", "year", "open", "high", "low", "close", "volume")
        .repartition("symbol")
        .sortWithinPartitions("symbol", "date")
        .persist()
    )

    # time windows
    w_sym_order = Window.partitionBy("symbol").orderBy("date")
    w_5 = w_sym_order.rowsBetween(-4, 0)
    w_21 = w_sym_order.rowsBetween(-20, 0)
    w_63 = w_sym_order.rowsBetween(-62, 0)
    w_252 = w_sym_order.rowsBetween(-251, 0)
    w_14 = w_sym_order.rowsBetween(-13, 0)

    # 1-day returns / log-returns
    df = df_daily.withColumn(
        "prev_close", F.lag("close", 1).over(w_sym_order)
    )

    df = df.withColumn(
        "ret_1d",
        F.when(
            F.col("prev_close").isNotNull(),
            F.col("close") / F.col("prev_close") - F.lit(1.0),
        ).otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "log_ret_1d",
        F.when(
            (F.col("prev_close").isNotNull()) & (F.col("prev_close") > 0),
            F.log(F.col("close") / F.col("prev_close")),
        ).otherwise(F.lit(None).cast("double")),
    )

    # N-day returns (5, 21, 63, 252)

    def rolling_logret_to_ret(col_name, window):
        return F.exp(F.sum(F.col(col_name)).over(window)) - F.lit(1.0)

    df = (
        df.withColumn("ret_5d", rolling_logret_to_ret("log_ret_1d", w_5))
          .withColumn("ret_21d", rolling_logret_to_ret("log_ret_1d", w_21))
          .withColumn("ret_63d", rolling_logret_to_ret("log_ret_1d", w_63))
          .withColumn("ret_252d", rolling_logret_to_ret("log_ret_1d", w_252))
    )

    # Volatility (annualized)
    df = df.withColumn(
        "vol_21d",
        F.sqrt(F.lit(252.0)) * F.stddev_samp("log_ret_1d").over(w_21),
    ).withColumn(
        "vol_63d",
        F.sqrt(F.lit(252.0)) * F.stddev_samp("log_ret_1d").over(w_63),
    )

    # moving averages / price z-score
    df = (
        df.withColumn("ma_21d", F.avg("close").over(w_21))
          .withColumn("ma_63d", F.avg("close").over(w_63))
          .withColumn("ma_252d", F.avg("close").over(w_252))
    )

    df = df.withColumn(
        "price_z_21d",
        (F.col("close") - F.avg("close").over(w_21))
        / F.stddev_samp("close").over(w_21),
    ).withColumn(
        "price_z_63d",
        (F.col("close") - F.avg("close").over(w_63))
        / F.stddev_samp("close").over(w_63),
    ).withColumn(
        "price_z_252d",
        (F.col("close") - F.avg("close").over(w_252))
        / F.stddev_samp("close").over(w_252),
    )

    df = df.withColumn(
        "price_vs_ma_252d",
        F.when(
            F.col("ma_252d").isNotNull() & (F.col("ma_252d") != 0),
            F.col("close") / F.col("ma_252d") - F.lit(1.0),
        ).otherwise(F.lit(None).cast("double")),
    )

    # drawdown + new high/low flags
    df = df.withColumn(
        "rolling_max_252d", F.max("close").over(w_252)
    ).withColumn(
        "rolling_min_252d", F.min("close").over(w_252)
    ).withColumn(
        "drawdown_252d",
        F.when(
            F.col("rolling_max_252d").isNotNull()
            & (F.col("rolling_max_252d") != 0),
            F.col("close") / F.col("rolling_max_252d") - F.lit(1.0),
        ).otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "is_new_high_252d",
        F.when(F.col("close") == F.col("rolling_max_252d"), F.lit(1)).otherwise(F.lit(0)),
    ).withColumn(
        "is_new_low_252d",
        F.when(F.col("close") == F.col("rolling_min_252d"), F.lit(1)).otherwise(F.lit(0)),
    )

    # volume – moving average and z-score
    df = (
        df.withColumn("volume_ma_21d", F.avg("volume").over(w_21))
          .withColumn(
              "volume_z_21d",
              (F.col("volume") - F.avg("volume").over(w_21))
              / F.stddev_samp("volume").over(w_21),
          )
    )

    # RSI 14
    df = df.withColumn(
        "gain_1d",
        F.when(F.col("ret_1d") > 0, F.col("ret_1d")).otherwise(F.lit(0.0)),
    ).withColumn(
        "loss_1d",
        F.when(F.col("ret_1d") < 0, -F.col("ret_1d")).otherwise(F.lit(0.0)),
    )

    df = (
        df.withColumn("avg_gain_14d", F.avg("gain_1d").over(w_14))
          .withColumn("avg_loss_14d", F.avg("loss_1d").over(w_14))
    )

    df = df.withColumn(
        "rsi_14d",
        F.when(F.col("avg_loss_14d") == 0, F.lit(100.0))
         .otherwise(
             100.0 - (100.0 / (1.0 + (F.col("avg_gain_14d") / F.col("avg_loss_14d"))))
         ),
    )

    # relative performance vs SPY (21d)
    # first, extract SPY only
    df_spy = (
        df.filter(F.col("symbol") == "SPY")
          .select("date", F.col("ret_21d").alias("ret_21d_spy"))
    )

    df = (
        df.join(df_spy, on="date", how="left")
          .withColumn("ret_21d_vs_spy", F.col("ret_21d") - F.col("ret_21d_spy"))
    )

    # simple features for crash / bubble / big-move narratives
    # large 1-day moves (e.g. for heatmaps / scatter plots)
    df = df.withColumn(
        "is_big_drop_1d",
        F.when(F.col("ret_1d") <= -0.05, F.lit(1)).otherwise(F.lit(0)),
    ).withColumn(
        "is_big_rally_1d",
        F.when(F.col("ret_1d") >= 0.05, F.lit(1)).otherwise(F.lit(0)),
    )

    # simple crash signal over 21 days (e.g. -20% or worse)
    df = df.withColumn(
        "is_crash_21d",
        F.when(F.col("ret_21d") <= -0.20, F.lit(1)).otherwise(F.lit(0)),
    )

    # “Bubble” signal – far above long-term MA and not yet broken
    # price_vs_ma_252d = (close / ma_252d - 1)
    # e.g. > +50% above 252-day MA and drawdown no worse than -10%
    df = df.withColumn(
        "is_bubble_252d",
        F.when(
            (F.col("price_vs_ma_252d") >= 0.50) &
            (F.col("drawdown_252d") >= -0.10),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    # simple “regime label” for chart narratives
    # can be used as color / legend in BI tools
    df = df.withColumn(
        "regime_label_21d",
        F.when(F.col("is_crash_21d") == 1, F.lit("crash"))
         .when(F.col("is_bubble_252d") == 1, F.lit("bubble"))
         .otherwise(F.lit("normal")),
    )

    # drop temporary helper columns
    drop_cols = [
        "prev_close", "gain_1d", "loss_1d",
        "avg_gain_14d", "avg_loss_14d",
        "ret_21d_spy",
    ]
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)

    # repartition + write
    df = df.repartition("year")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # rename for BI / Athena / QuickSight consistency
    df = df.withColumnRenamed("date", "trade_date")

    (
        df.write
          .partitionBy("year")
          .mode("overwrite")
          .parquet(features_base)
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