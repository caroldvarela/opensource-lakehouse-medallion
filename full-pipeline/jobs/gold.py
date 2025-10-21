from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    col,
    count,
    sum,
    when,
    coalesce,
    lit,
    regexp_replace
)


CATALOG_URI = "http://nessie:19120/api/v1"
WAREHOUSE = "s3a://gold/"
STORAGE_URI = "http://minio:9000"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "password"

# ======================
# 1. Spark + Nessie Configuration
# ======================
spark = (
    SparkSession.builder
    .appName("spark_gold")
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
    # Gold optimization configurations
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    .getOrCreate()
)


# ======================
# 2. Create GOLD namespace
# ======================
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")


# ======================
# 3. Read Silver tables with filters to optimize performance
# ======================
print("Loading Silver data with optimization filters...")

# Posts: filter by recent year and posts with activity
posts = (
    spark.read.table("nessie.silver.posts")
    .filter(col("anio") >= "2020")  # Adjust if 'anio' is numeric
    .filter(col("score").isNotNull())
    .filter(col("owner_user_id").isNotNull())
)
print(f"Filtered posts: {posts.count()} rows")

# Votes: filter by post_id and relevant vote types
votes = (
    spark.read.table("nessie.silver.votes")
    .filter(col("post_id").isNotNull())
    .filter(col("vote_type_id").isin([2, 3]))  # Only upvotes (2) and downvotes (3)
)
print(f"Filtered votes: {votes.count()} rows")

# Comments: filter by post_id and user_id
comments = (
    spark.read.table("nessie.silver.comments")
    .filter(col("post_id").isNotNull())
    .filter(col("user_id").isNotNull())
)
print(f"Filtered comments: {comments.count()} rows")

# Users: robust casting of 'reputation' (comes as BINARY in your case) to long
users_raw = spark.read.table("nessie.silver.users")

# -- Optional for debugging (comment in production if expensive)
users_raw.printSchema()
users_raw.select("reputation").show(10, truncate=False)

# Clean and cast reputation: remove non-numeric characters and cast to long
users = (
    users_raw
    .filter(col("id").isNotNull())
    .withColumn("reputation_str", regexp_replace(col("reputation").cast("string"), r"[^0-9]", ""))
    .withColumn("reputation_long", col("reputation_str").cast("long"))
    .filter(col("reputation_long") > 0)
    .drop("reputation", "reputation_str")
    .withColumnRenamed("reputation_long", "reputation")
)
print(f"Filtered users: {users.count()} rows")

# Badges: filter by existing user and name
badges = (
    spark.read.table("nessie.silver.badges")
    .filter(col("user_id").isNotNull())
    .filter(col("name").isNotNull())
)
print(f"Filtered badges: {badges.count()} rows")


# ======================
# 4. Generic MERGE function for Gold (improved for PK mapping)
# ======================
def merge_gold(df, table_name, pk_cols):
    """
    Robust merge to nessie.gold.<table_name>.
    - If table doesn't exist, creates it (adding fecha_cargue).
    - If exists, tries to map common PK names between df and target table
      (id, user_id, owner_user_id, owner_id).
    - Validates that resulting PKs exist in both staging (df) and target table.
    """
    target_table = f"nessie.gold.{table_name}"
    
    if not spark.catalog.tableExists(target_table):
        # Create table for first time with fecha_cargue
        df_gold = df.withColumn("fecha_cargue", current_timestamp())
        (
            df_gold.writeTo(target_table)
            .tableProperty("format-version", "2")
            .create()
        )
        print(f"Gold table created: {target_table}")
        return

    # If table already exists: adapt columns
    target_cols = spark.table(target_table).columns
    df_cols = df.columns
    df_for_merge = df

    # Simple mapping rule: if requested pk doesn't exist in target,
    # try to find equivalent in target and rename in staging.
    # Allow several common aliases.
    aliases = ["id", "user_id", "owner_user_id", "owner_id"]

    # Build final list of PKs that will be used in MERGE ON clause
    final_pk_cols = []
    for pk in pk_cols:
        if pk in target_cols and pk in df_for_merge.columns:
            final_pk_cols.append(pk)
            continue

        # If pk is not in target, try to find candidate in target
        # that makes sense and rename column in df_for_merge to that name.
        mapped = None

        # Common case: df has 'id' but target has 'owner_user_id' (or user_id)
        if 'id' in df_for_merge.columns:
            for candidate in ['owner_user_id', 'user_id', 'owner_id']:
                if candidate in target_cols and candidate not in df_for_merge.columns:
                    df_for_merge = df_for_merge.withColumnRenamed('id', candidate)
                    mapped = candidate
                    break

        # If pk is 'id' and df has owner_user_id, but target expects 'id'
        if mapped is None and pk == 'id' and 'owner_user_id' in df_for_merge.columns and 'id' in target_cols:
            df_for_merge = df_for_merge.withColumnRenamed('owner_user_id', 'id')
            mapped = 'id'

        # If still not mapped, try to map columns by substring
        if mapped is None:
            # search column in df that contains same keyword
            match = next((c for c in df_for_merge.columns if pk in c and c in target_cols), None)
            if match:
                mapped = match

        # If mapped was found, use it; if not, check if pk is already in target_cols (but df lacks it)
        if mapped:
            final_pk_cols.append(mapped)
        else:
            # If pk is in target_cols but not in df, try to find equivalent in df and rename to pk
            if pk in target_cols:
                candidate_in_df = next((c for c in df_for_merge.columns if c in aliases and c != pk), None)
                if candidate_in_df:
                    df_for_merge = df_for_merge.withColumnRenamed(candidate_in_df, pk)
                    final_pk_cols.append(pk)
                    continue

            # Could not resolve pk automatically -> throw informative error
            raise RuntimeError(
                f"Cannot resolve PK column '{pk}' for MERGE into {target_table}.\n"
                f"Target columns: {target_cols}\n"
                f"Staging columns: {df.columns}\n"
                "I attempted common mappings (id <-> owner_user_id/user_id) but failed. "
                "Please pass pk_cols that match the target table column names or rename the staging DF accordingly."
            )

    # At this point, final_pk_cols contains column names that DO exist in target and staging
    # Create temporary view and execute MERGE
    temp_view = f"staging_{table_name}"
    df_for_merge.createOrReplaceTempView(temp_view)

    # join condition
    join_cond = " AND ".join([f"t.{c}=s.{c}" for c in final_pk_cols])

    # Columns to update/insert: use staging columns that are also in target (avoid unexpected columns)
    cols = [c for c in df_for_merge.columns if c != "fecha_cargue" and c in target_cols]
    if not cols:
        raise RuntimeError(f"No shared columns between staging and target for table {target_table}. Target cols: {target_cols}, staging cols: {df_for_merge.columns}")

    update_columns = ", ".join([f"t.{c} = s.{c}" for c in cols])
    insert_columns = ", ".join(cols)
    insert_values = ", ".join([f"s.{c}" for c in cols])

    sql = f"""
        MERGE INTO {target_table} t
        USING {temp_view} s
        ON {join_cond}
        WHEN MATCHED THEN UPDATE SET {update_columns}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
    """
    print(f"Executing MERGE in {target_table} with PKs {final_pk_cols} and columns: {cols}")
    spark.sql(sql)
    print(f"Gold table updated: {target_table}")


# ======================
# 5. Gold KPIs
# ======================

# 5.1 post_counts_by_user - OPTIMIZED: Only users with recent posts
print("Calculating post_counts_by_user...")
post_counts_by_user = (
    posts
    .filter(col("owner_user_id").isNotNull())
    .groupBy("owner_user_id")
    .agg(
        sum(when(col("post_type_id") == 1, 1).otherwise(0)).alias("num_questions"),
        sum(when(col("post_type_id") == 2, 1).otherwise(0)).alias("num_answers"),
        count("*").alias("total_posts")
    )
    .filter(col("total_posts") > 0)
    # rename key for later joins: owner_user_id -> id (if you want gold to have 'id')
    .withColumnRenamed("owner_user_id", "id")
)
merge_gold(post_counts_by_user, "post_counts_by_user", ["id"])

# 5.2 vote_stats_per_post - OPTIMIZED: Only posts with votes
print("Calculating vote_stats_per_post...")
vote_stats_per_post = (
    votes
    .filter(col("post_id").isNotNull())
    .groupBy("post_id")
    .agg(
        sum(when(col("vote_type_id") == 2, 1).otherwise(0)).alias("upvotes"),
        sum(when(col("vote_type_id") == 3, 1).otherwise(0)).alias("downvotes"),
        count("*").alias("total_votes")
    )
    .filter(col("total_votes") > 0)
)
merge_gold(vote_stats_per_post, "vote_stats_per_post", ["post_id"])

# 5.3 top_tags - OPTIMIZED: Only tags with many questions
print("Calculating top_tags...")
top_tags = (
    posts
    .filter(col("tags").isNotNull())
    .filter(col("tags") != "")
    .filter(col("post_type_id") == 1)  # Only questions
    .withColumn("tag", col("tags"))
    .groupBy("tag")
    .agg(count("*").alias("num_questions"))
    .filter(col("num_questions") >= 5)
    .orderBy(col("num_questions").desc())
    .limit(1000)
)
merge_gold(top_tags, "top_tags", ["tag"])

# 5.4 user_engagement - OPTIMIZED: Calculate metrics separately and then join
print("Calculating user_engagement in optimized way...")

# Calculate metrics separately and rename key to 'id' to avoid ambiguity in joins
user_posts = (
    posts.groupBy("owner_user_id")
    .agg(count("*").alias("total_posts"))
    .withColumnRenamed("owner_user_id", "id")
)

user_comments = (
    comments.groupBy("user_id")
    .agg(count("*").alias("total_comments"))
    .withColumnRenamed("user_id", "id")
)

user_votes = (
    votes.groupBy("user_id")
    .agg(count("*").alias("total_votes"))
    .withColumnRenamed("user_id", "id")
)

user_badges = (
    badges.groupBy("user_id")
    .agg(count("*").alias("total_badges"))
    .withColumnRenamed("user_id", "id")
)

# Join using 'id' column (no more ambiguity)
user_engagement = (
    users.select("id")
    .join(user_posts, on="id", how="left")
    .join(user_comments, on="id", how="left")
    .join(user_votes, on="id", how="left")
    .join(user_badges, on="id", how="left")
    .select(
        col("id"),
        coalesce(col("total_posts"), lit(0)).alias("total_posts"),
        coalesce(col("total_comments"), lit(0)).alias("total_comments"),
        coalesce(col("total_votes"), lit(0)).alias("total_votes"),
        coalesce(col("total_badges"), lit(0)).alias("total_badges")
    )
)
merge_gold(user_engagement, "user_engagement", ["id"])

# 5.5 badges_summary - OPTIMIZED: Only badges with active users
print("Calculating badges_summary...")
badges_summary = (
    badges
    .filter(col("user_id").isNotNull())
    .filter(col("name").isNotNull())
    .groupBy("user_id", "name")
    .agg(count("*").alias("num_badges"))
    .filter(col("num_badges") > 0)
)
merge_gold(badges_summary, "badges_summary", ["user_id", "name"])

print("Gold tables generated in Iceberg (nessie.gold.*)")
