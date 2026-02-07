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
            "DAILY_FEATURES_PATH",
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

    features_path = args["DAILY_FEATURES_PATH"]
    dim_path = args["DIM_SYMBOL_PATH"]
    output_path = args["OUTPUT_PATH"].rstrip("/")

    year_start = int(args["YEAR_START"])
    year_end = int(args["YEAR_END"])
    years_span = year_end - year_start + 1

    # light job -> low shuffle
    if years_span <= 1:
        shuffle_parts = 32
    elif years_span <= 5:
        shuffle_parts = 64
    elif years_span <= 10:
        shuffle_parts = 96
    else:
        shuffle_parts = 128

    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_parts))

    # read daily features
    df_features = (
        spark.read.parquet(features_path)
        .filter(F.col("year").between(year_start, year_end))
    )

    # read dim_symbol (small table)
    df_dim = (
        spark.read.option("header", "true")
        .csv(dim_path)
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
    df_dim = df_dim.withColumn("data_priority", F.col("data_priority").cast("int"))

    # join (broadcast implicit)
    df = df_features.join(df_dim, on="symbol", how="left")

    # write one Parquet file per year
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df = df.persist()

    years = (
        df.select("year")
          .distinct()
          .orderBy("year")
          .rdd.map(lambda r: r["year"])
          .collect()
    )

    for y in years:
        (
            df.filter(F.col("year") == y)
              .coalesce(1)  # exactly one file per year
              .write
              .mode("overwrite")
              .parquet(f"{output_path}/year={y}")
        )

    df.unpersist()
    job.commit()


if __name__ == "__main__":
    main()