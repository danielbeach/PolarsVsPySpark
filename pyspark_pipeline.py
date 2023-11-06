import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime


def read_remote_csvs(spark: SparkSession, 
                     path: str
                     ) -> DataFrame:
    df = spark.read.option("recursiveFileLookup", "true").csv(path, header='true')
    return df


def read_parquets(spark: SparkSession, 
                    path: str) -> DataFrame:
    df = spark.read.format("parquet").load(path)
    return df


def write_parquets(df: DataFrame, path: str) -> None:
    df.repartition('date').write.partitionBy('date').format("parquet").mode("overwrite").save(path)


def calculate_metrics(df: DataFrame) -> DataFrame:
    metrics = df.groupBy(F.col('date'), 
                         F.year('date').alias('year'),
                         F.month('date').alias('month'),
                         F.dayofmonth('date').alias('day'),
                         F.col('model')
                         ).agg(F.sum("failure").alias("failures")
                            )
    return metrics


def main():
    t1 = datetime.now()
    read_path = '/root/data/*/*.csv'
    write_path = "/root/data/parquets/"
    metrics_write_path = "root/data/hard_drive_failure_metrics"
    spark = SparkSession.builder.master("local[6]") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
    raw_data = read_remote_csvs(spark, read_path)
    raw_data = raw_data.select("date", "model", "failure")
    write_parquets(raw_data, write_path)

    data = read_parquets(spark, write_path)
    metrics = calculate_metrics(data)
    write_parquets(metrics, metrics_write_path)

    t2 = datetime.now()
    print(f"Time to write raw data to Delta Lake: {t2 - t1}") 


if __name__ == "__main__":
    main()