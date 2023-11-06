import polars as pl
from datetime import datetime


def read_csvs(path: str):
    lazy_df = pl.scan_csv(path)
    return lazy_df


def calculate_metrics(df):
    metrics = \
    df.groupby(pl.col('date').cast(pl.Date), 
        pl.col('date').cast(pl.Date).dt.year().alias('year'),
        pl.col('date').cast(pl.Date).dt.month().alias('month'),
        pl.col('date').cast(pl.Date).dt.day().alias('day'),
        pl.col('model')
        ).agg(pl.sum("failure").alias("failures")
        )
    return metrics


def write_parquets(lz, path: str) -> None:
    lz.sink_parquet(path,
                    compression='snappy',
                    )
    

def read_parquets(path: str):
    lz = pl.scan_parquet(path)
    return lz

def main():
    t1 = datetime.now()
    read_path = '/root/data/*/*.csv'
    write_path = "./parquets"
    metrics_write_path = "./hard_drive_failure_metrics"

    lz = read_csvs(read_path)
    lz = lz.select(["date", "model", "failure"])
    write_parquets(lz, write_path)

    raw_data = read_parquets(write_path)

    metrics = calculate_metrics(raw_data)
    write_parquets(metrics, metrics_write_path)
    t2 = datetime.now()
    print(f"Time to run polars pipeline : {t2 - t1}")


if __name__ == "__main__":
    main()