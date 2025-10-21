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

    # === Year-Parameterized Resource Generator Function ===
    def make_yearly_resource(table_name, base_url, years):
        resources = []
        for year in years:
            @dlt.resource(table_name=f"{table_name}_{year}", parallelized=True)
            def resource(year=year, base_url=base_url):
                url = f"{base_url}/{year}.parquet"
                yield from parquet_batches(url)
            resources.append(resource)
        return resources

    # === Define Tables by Year ===
    posts_resources = make_yearly_resource(
        "posts",
        "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts",
        years=[2019, 2020]
    )

    votes_resources = make_yearly_resource(
        "votes",
        "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/votes",
        years=[2019, 2020]
    )

    comments_resources = make_yearly_resource(
        "comments",
        "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/comments",
        years=[2019, 2020]
    )

    posthistory_resources = make_yearly_resource(
        "posthistory",
        "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posthistory",
        years=[2019, 2020]
    )

    # === Tables That Do Not Depend on the Year ===
    @dlt.resource(table_name="users", parallelized=True)
    def users():
        url = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet"
        yield from parquet_batches(url)

    @dlt.resource(table_name="badges", parallelized=True)
    def badges():
        url = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/badges.parquet"
        yield from parquet_batches(url)

    @dlt.resource(table_name="postlinks", parallelized=True)
    def postlinks():
        url = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/postlinks.parquet"
        yield from parquet_batches(url)

    # Pipeline configured with filesystem as the destination
    pipeline = dlt.pipeline(
        pipeline_name="parquet_to_minio",
        destination="filesystem",
        dataset_name="stackoverflow",
    )

    # We combine all resources into a single list
    all_resources = (
        posts_resources
        + votes_resources
        + comments_resources
        + posthistory_resources
        + [users, badges, postlinks]
    )

    # We run the pipeline with all the tables
    load_info = pipeline.run(
        all_resources,
        loader_file_format="parquet",
        write_disposition="replace"
    )

    print(load_info)
