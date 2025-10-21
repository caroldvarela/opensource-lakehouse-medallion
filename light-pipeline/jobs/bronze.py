import dlt
import fsspec
import pyarrow.parquet as pq
import pyarrow


def execute_bronze():
    def parquet_batches(url, batch_size=50_000):
        with fsspec.open(url, mode="rb") as f:
            parquet_file = pq.ParquetFile(f)
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                yield batch.to_pandas()

    # Only one table per requirement: votes from 2021
    @dlt.resource(table_name="votes_2021", parallelized=True)
    def votes_2021():
        url = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/votes/2021.parquet"
        yield from parquet_batches(url)

    # Pipeline to filesystem (bronze in MinIO defined in secrets.toml)
    pipeline = dlt.pipeline(
        pipeline_name="parquet_to_minio",
        destination="filesystem",
        dataset_name="stackoverflow",
    )

    load_info = pipeline.run(
        [votes_2021],
        loader_file_format="parquet",
        write_disposition="replace"
    )

    print(load_info)
