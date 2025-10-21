from pyspark.sql.functions import input_file_name, regexp_extract, current_timestamp
from pyspark.sql import SparkSession
import time

CATALOG_URI = "http://nessie:19120/api/v1"
WAREHOUSE = "s3a://silver/"
STORAGE_URI = "http://minio:9000"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "password"

# ======================
# 1. Spark + Nessie Configuration
# ======================
spark = (
    SparkSession.builder
    .appName("spark_silver")
    .master("spark://spark-master:7077")
    # Iceberg + Nessie Configuration
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.uri", CATALOG_URI)
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.authentication.type", "NONE")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.s3.endpoint", STORAGE_URI)
    .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
    .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    # S3/MinIO Configuration
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", STORAGE_URI)
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    # Memory optimization configurations
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    # Conservative driver/executor memory to avoid container OOM
    .config("spark.executor.memory", "2g")
    .getOrCreate()
)

# ======================
# 2. Read Bronze
# ======================
bronze_path = "s3a://bronze/stackoverflow"

# Adjust patterns to read from subfolders
tables = {
    "posts": f"{bronze_path}/posts_*/*.parquet",
    "votes": f"{bronze_path}/votes_*/*.parquet"
}

# Note: 'badges' removed because it doesn't exist in the bucket

# ======================
# 3. Generic function: normalize + merge to Silver
# ======================
def write_silver(table_name, df, pk="Id"):
    """Load a Silver table in Iceberg with MERGE INTO"""
    target_table = f"nessie.silver.{table_name}"
    
    try:
        print(f"Processing {table_name} - Rows: {df.count()}")
        
        # Only add duplicates, without load_date to avoid determinism issues
        df_silver = df.dropDuplicates([pk])
        print(f"After deduplication: {df_silver.count()} rows")

        if not spark.catalog.tableExists(target_table):
            print(f"Creating new table: {target_table}")
            # Add load_date only on initial creation
            df_with_timestamp = df_silver.withColumn("load_date", current_timestamp())
            (
                df_with_timestamp.writeTo(target_table)
                .tableProperty("format-version", "2")
                .create()
            )
            print(f"Table created: {target_table}")
        else:
            print(f"Updating existing table: {target_table}")
            # For MERGE, use only the original columns without load_date
            temp_view = f"staging_{table_name}"
            df_silver.createOrReplaceTempView(temp_view)

            # Columns excluding load_date for MERGE
            columns = [col for col in df_silver.columns if col != "load_date"]
            update_columns = ", ".join([f"t.{col} = s.{col}" for col in columns])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"s.{col}" for col in columns])

            merge_sql = f"""
                MERGE INTO {target_table} t
                USING {temp_view} s
                ON t.{pk} = s.{pk}
                WHEN MATCHED THEN UPDATE SET {update_columns}
                WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
            """
            print(f"Executing MERGE SQL for {target_table}")
            spark.sql(merge_sql)
            print(f"Table updated with MERGE: {target_table}")
            
    except Exception as e:
        print(f"Error processing {table_name}: {str(e)}")
        raise e

# ======================
# 4. Create Silver namespace in Nessie
# ======================
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

# ======================
# 5. Process all tables
# ======================

# Partitioned tables by year → extract "year" from file name
def read_with_year(path, regex=".*_(\\d+)\\/.*"):
    df = spark.read.parquet(path)
    print(f"Reading {path}: {df.count()} rows")
    # Add year column and materialize by writing to a temporary table
    df_with_year = df.withColumn("year", regexp_extract(input_file_name(), regex, 1))
    
    # Create a temporary table to materialize non-deterministic expression
    temp_table_name = f"temp_posts_{int(time.time())}"
    temp_table = f"nessie.silver.{temp_table_name}"
    
    try:
        # Write to temporary table to materialize data
        df_with_year.writeTo(temp_table).create()
        # Read back materialized data
        materialized_df = spark.read.table(temp_table)
        return materialized_df
    finally:
        # Clean up temporary table
        try:
            spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
        except:
            pass  # Ignore cleanup errors

spark.catalog.clearCache()

# Process tables one by one to optimize memory
print("Processing table: posts")
df_posts = read_with_year(tables["posts"])
write_silver("posts", df_posts, pk="Id")
df_posts.unpersist()  # Free memory

print("Processing table: votes")
df_votes = read_with_year(tables["votes"])
write_silver("votes", df_votes, pk="Id")
df_votes.unpersist()  # Free memory

print("All Silver tables loaded into Iceberg with Nessie")
