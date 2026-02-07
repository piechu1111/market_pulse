import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql import functions as F


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "FEATURES_PATH",
            "DIM_SYMBOL_PATH",
            "OUTPUT_PATH",
            "YEAR_START",
            "YEAR_END",
        ],
    )

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    features_path = args["FEATURES_PATH"]
    dim_symbol_path = args["DIM_SYMBOL_PATH"]
    output_path = args["OUTPUT_PATH"]

    year_start = int(args["YEAR_START"])
    year_end = int(args["YEAR_END"])
    years_span = year_end - year_start + 1

    # heuristic mapping shuffle partitions - light job, therefore low values
    if years_span <= 1:
        shuffle_parts = 32
    elif years_span <= 5:
        shuffle_parts = 64
    elif years_span <= 10:
        shuffle_parts = 96
    else:
        shuffle_parts = 128

    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_parts))
    print(f"shuffle partitions = {shuffle_parts}")


    # read DAILY FEATURES
    df = (
        spark.read.parquet(features_path)
        .filter(F.col("year").between(year_start, year_end))
        .select(
            "symbol",
            "trade_date",
            "year",
            "vol_21d",
            "price_z_252d",
            "volume_z_21d",
            "price_vs_ma_252d",
            "drawdown_252d",
        )
    )

    # read DIM SYMBOL (CSV)
    dim_symbol = (
        spark.read.option("header", "true")
        .csv(dim_symbol_path)
        .select(
            "symbol",
            "asset_type",
            "category",
            "risk_group",
            "liquidity_class",
            "systemic_role",
            "narrative_role",
            "data_priority",
        )
    )
    
    # normalize data types
    dim_symbol = dim_symbol.withColumn("data_priority", F.col("data_priority").cast("int"))

    # percentyl zmienności (per symbol)
    df_vol_p90 = (
        df.groupBy("symbol")
          .agg(F.expr("percentile_approx(vol_21d, 0.9)").alias("vol_21d_p90"))
    )

    df = df.join(df_vol_p90, on="symbol", how="left")

    # bubble heuristics
    df = df.withColumn(
        "is_bubble_zone",
        F.when(
            (F.col("price_z_252d") > 3) &
            (F.col("volume_z_21d") > 2) &
            (F.col("price_vs_ma_252d") > 0.5),
            1
        ).otherwise(0)
    )

    # crash / stress heuristics
    df = df.withColumn(
        "is_crash_zone",
        F.when(
            (F.col("drawdown_252d") < -0.30) &
            (F.col("vol_21d") > F.col("vol_21d_p90")),
            1
        ).otherwise(0)
    )

    # bubble score (0–100)
    df = df.withColumn(
        "bubble_score",
        F.least(
            F.lit(100.0),
            F.greatest(
                F.lit(0.0),
                20 * F.col("price_z_252d")
                + 15 * F.col("volume_z_21d")
                + 10 * F.col("price_vs_ma_252d")
                - 5 * F.col("drawdown_252d")
            ),
        )
    )

    # JOIN with DIM SYMBOL (Spark will do broadcast)
    df = df.join(dim_symbol, on="symbol", how="left")

    # final schema (dashboard-friendly)
    df_out = df.select(
        "symbol",
        "trade_date",
        "year",

        "asset_type",
        "category",
        "risk_group",
        "liquidity_class",
        "systemic_role",
        "narrative_role",
        "data_priority",

        "is_bubble_zone",
        "is_crash_zone",
        "bubble_score",

        "price_z_252d",
        "volume_z_21d",
        "price_vs_ma_252d",
        "drawdown_252d",
        "vol_21d",
        "vol_21d_p90",
    )

    # write GOLD
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # cache to avoid calculating df_out many times
    df_out = df_out.persist()

    # list year to write
    years = (
        df_out.select("year")
              .distinct()
              .orderBy("year")
              .rdd.map(lambda r: r["year"])
              .collect()
    )

    base_path = output_path.rstrip("/")  # just in case

    for year in years:
        print(f"Writing year={year} ...")

        (
            df_out
            .filter(F.col("year") == year)
            .coalesce(1)  # we want 1 output file per year for further cost optimalization
            .write
            # creating year catalog manually year=YYYY
            .mode("overwrite")
            .parquet(f"{base_path}/year={year}/")
        )

    df_out.unpersist()

    job.commit()


if __name__ == "__main__":
    main()