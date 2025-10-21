from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    col,
    count,
    sum,
    when,
    coalesce,
    lit,
    regexp_replace,
    month,
    split,
    explode,
    trim,
    length,
    avg
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
    # Optimization Configurations for Gold
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
# 2. Create GOLD Namespace
# ======================
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")


# ======================
# 3. Read Silver tables with filters to optimize performance
# ======================
print("Cargando datos Silver con filtros de optimización...")

# Posts: filter only for year 2021 and posts with activity
posts = (
    spark.read.table("nessie.silver.posts")
    .filter(col("year") == "2021")  # Only 2021 data
    .filter(col("Score").isNotNull())
    .filter(col("OwnerUserId").isNotNull())
)
print(f"Posts filtrados (2021): {posts.count()} filas")

# Debug: Check some example posts
print("Ejemplo de posts:")
posts.select("Id", "OwnerUserId", "PostTypeId", "Score", "year").show(5)

# Votes: filter by post_id and relevant vote types
votes = (
    spark.read.table("nessie.silver.votes")
    .filter(col("PostId").isNotNull())
    .filter(col("VoteTypeId").isin([2, 3]))  # Only upvotes (2) and downvotes (3)
)
print(f"Votes filtrados: {votes.count()} filas")

# Debug: Check some example votes
print("Ejemplo de votes:")
votes.select("PostId", "VoteTypeId", "UserId").show(5)

# ======================
# 4. Generic MERGE function for Gold (improved to map PKs)
# ======================
def merge_gold(df, table_name, pk_cols):
    """
    Robust merge into nessie.gold.<table_name>.
    - If the table doesn't exist, create it (adding load_date).
    - If it exists, try to map common PK names between df and the target table
      (id, user_id, owner_user_id, owner_id).
    - Validates that the resulting PKs exist both in staging (df) and the target table.
    """
    target_table = f"nessie.gold.{table_name}"
    
    if not spark.catalog.tableExists(target_table):
        # Create table for the first time with load_date
        df_gold = df.withColumn("load_date", current_timestamp())
        (
            df_gold.writeTo(target_table)
            .tableProperty("format-version", "2")
            .create()
        )
        print(f"Tabla Gold creada: {target_table}")
        return

    # If the table already exists: adapt columns
    target_cols = spark.table(target_table).columns
    df_cols = df.columns
    df_for_merge = df

    # Simple mapping rule: if the requested pk doesn't exist in target,
    # try to find an equivalent in target and rename in staging.
    # Allow several common aliases.
    aliases = ["id", "user_id", "owner_user_id", "owner_id"]

    # Build the final list of PKs to use in the MERGE ON clause
    final_pk_cols = []
    for pk in pk_cols:
        if pk in target_cols and pk in df_for_merge.columns:
            final_pk_cols.append(pk)
            continue

        # If pk is not in target, try to find a suitable candidate in target
        # and rename the column in df_for_merge to that name.
        mapped = None

        # Common case: df has 'id' but target has 'owner_user_id' (or 'user_id')
        if 'id' in df_for_merge.columns:
            for candidate in ['owner_user_id', 'user_id', 'owner_id']:
                if candidate in target_cols and candidate not in df_for_merge.columns:
                    df_for_merge = df_for_merge.withColumnRenamed('id', candidate)
                    mapped = candidate
                    break

        # If pk is 'id' and df has 'owner_user_id', but target expects 'id'
        if mapped is None and pk == 'id' and 'owner_user_id' in df_for_merge.columns and 'id' in target_cols:
            df_for_merge = df_for_merge.withColumnRenamed('owner_user_id', 'id')
            mapped = 'id'

        # If still not mapped, try to map columns by substring
        if mapped is None:
            # Look for a column in df that contains the same keyword
            match = next((c for c in df_for_merge.columns if pk in c and c in target_cols), None)
            if match:
                mapped = match

        # If mapped was found, use it; otherwise, check if pk already exists in target_cols (but df lacks it)
        if mapped:
            final_pk_cols.append(mapped)
        else:
            # If pk is in target_cols but not in df, try to find an equivalent in df and rename it to pk
            if pk in target_cols:
                candidate_in_df = next((c for c in df_for_merge.columns if c in aliases and c != pk), None)
                if candidate_in_df:
                    df_for_merge = df_for_merge.withColumnRenamed(candidate_in_df, pk)
                    final_pk_cols.append(pk)
                    continue

            # Could not automatically resolve the pk -> raise informative error
            raise RuntimeError(
                f"Cannot resolve PK column '{pk}' for MERGE into {target_table}.\n"
                f"Target columns: {target_cols}\n"
                f"Staging columns: {df.columns}\n"
                "Attempted common mappings (id <-> owner_user_id/user_id) but failed. "
                "Please provide pk_cols that match target table column names or rename staging DF accordingly."
            )

    # At this point, final_pk_cols contains the columns that exist in both target and staging
    temp_view = f"staging_{table_name}"
    df_for_merge.createOrReplaceTempView(temp_view)

    # join condition
    join_cond = " AND ".join([f"t.{c}=s.{c}" for c in final_pk_cols])

    # Columns to update/insert: use columns from staging that also exist in target (avoid unexpected columns)
    cols = [c for c in df_for_merge.columns if c != "load_date" and c in target_cols]
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
    print(f"Ejecutando MERGE en {target_table} con PKs {final_pk_cols} y columnas: {cols}")
    spark.sql(sql)
    print(f"Tabla Gold actualizada: {target_table}")


# ======================
# 5. Gold KPIs
# ======================
# 5.1 post_counts_by_user - OPTIMIZED: Only users with recent posts
print("Calculando post_counts_by_user...")
post_counts_by_user = (
    posts
    .filter(col("OwnerUserId").isNotNull())
    .groupBy("OwnerUserId")
    .agg(
        sum(when(col("PostTypeId") == 1, 1).otherwise(0)).alias("num_questions"),
        sum(when(col("PostTypeId") == 2, 1).otherwise(0)).alias("num_answers"),
        count("*").alias("total_posts")
    )
    .filter(col("total_posts") > 0)
    .withColumnRenamed("OwnerUserId", "id")
)
print(f"Post counts calculados: {post_counts_by_user.count()} filas")
merge_gold(post_counts_by_user, "post_counts_by_user", ["id"])


# 5.3 top_tags - OPTIMIZED: Only tags with many 2021 questions
print("Calculando top_tags...")
top_tags = (
    posts
    .filter(col("Tags").isNotNull())
    .filter(col("Tags") != "")
    .filter(col("PostTypeId") == 1) 
    .withColumn("tag", explode(split(col("Tags"), "<")))
    .withColumn("tag", trim(regexp_replace(col("tag"), ">", "")))
    .filter(col("tag") != "")
    .filter(length(col("tag")) > 1)
    .groupBy("tag")
    .agg(count("*").alias("num_questions"))
    .filter(col("num_questions") >= 5)
    .orderBy(col("num_questions").desc())
    .limit(1000)
)
print(f"Top tags calculados: {top_tags.count()} filas")
merge_gold(top_tags, "top_tags", ["tag"])

# 5.4 user_engagement - OPTIMIZED: Only post and vote metrics for 2021
print("Calculando user_engagement de forma optimizada...")

user_posts = (
    posts.groupBy("OwnerUserId")
    .agg(count("*").alias("total_posts"))
    .withColumnRenamed("OwnerUserId", "id")
)

user_votes = (
    votes.groupBy("UserId")
    .agg(count("*").alias("total_votes"))
    .withColumnRenamed("UserId", "id")
)

user_engagement = (
    user_posts
    .join(user_votes, on="id", how="outer")
    .select(
        col("id"),
        coalesce(col("total_posts"), lit(0)).alias("total_posts"),
        coalesce(col("total_votes"), lit(0)).alias("total_votes")
    )
)
print(f"User engagement calculado: {user_engagement.count()} filas")
merge_gold(user_engagement, "user_engagement", ["id"])

# 5.5 trending_topics_by_month - Most popular topics per month
print("Calculando trending_topics_by_month...")

trending_topics = (
    posts
    .filter(col("PostTypeId") == 1)  
    .filter(col("Tags").isNotNull())
    .filter(col("Tags") != "")
    .filter(col("CreationDate").isNotNull())
    .withColumn("month", month(col("CreationDate")))
    .withColumn("tag", explode(split(col("Tags"), "<")))
    .withColumn("tag", trim(regexp_replace(col("tag"), ">", "")))
    .filter(col("tag") != "")
    .filter(length(col("tag")) > 1)
    .groupBy("month", "tag")
    .agg(
        count("*").alias("question_count"),
        sum(col("Score")).alias("total_score"),
        avg(col("Score")).alias("avg_score")
    )
    .filter(col("question_count") >= 3) 
    .withColumn("month_name", 
        when(col("month") == 1, "Enero")
        .when(col("month") == 2, "Febrero")
        .when(col("month") == 3, "Marzo")
        .when(col("month") == 4, "Abril")
        .when(col("month") == 5, "Mayo")
        .when(col("month") == 6, "Junio")
        .when(col("month") == 7, "Julio")
        .when(col("month") == 8, "Agosto")
        .when(col("month") == 9, "Septiembre")
        .when(col("month") == 10, "Octubre")
        .when(col("month") == 11, "Noviembre")
        .when(col("month") == 12, "Diciembre")
    )
    .orderBy("month", col("question_count").desc())
)
print(f"Trending topics calculated: {trending_topics.count()} rows")
merge_gold(trending_topics, "trending_topics_by_month", ["month", "tag"])

print("Gold tables generated in Iceberg (nessie.gold.*) - Only 2021 data")