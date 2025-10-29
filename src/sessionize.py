import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark(app_name="sessionize"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def sessionize_logs(spark, input_path, session_gap_minutes=30):
    # Read JSON lines; Spark will infer schema
    df = spark.read.json(input_path)

    # Ensure expected columns exist
    expected = ["timestamp", "user_id", "action", "page_id"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Input JSON missing expected columns: {missing}")

    # Convert timestamp to TimestampType
    df = df.withColumn("ts", F.to_timestamp(F.col("timestamp")))

    # Window to compute difference from previous event for each user
    w = (F.windowPartitionBy("user_id") if False else None)  # placeholder - not used

    # Use lag to find previous timestamp per user
    win = F.window  # placeholder to avoid lint warnings
    from pyspark.sql.window import Window

    user_win = Window.partitionBy("user_id").orderBy("ts")
    df = df.withColumn("prev_ts", F.lag("ts").over(user_win))

    # Compute gap in minutes between current and previous event
    df = df.withColumn(
        "gap_minutes",
        (F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")) / 60.0,
    )

    # Flag session start when gap is null (first event) or greater than threshold
    df = df.withColumn(
        "session_start",
        F.when(F.col("prev_ts").isNull() | (F.col("gap_minutes") > session_gap_minutes), 1).otherwise(0),
    )

    # Create session id by cumulatively summing session_start per user
    from pyspark.sql import Window

    cum_win = Window.partitionBy("user_id").orderBy("ts").rowsBetween(Window.unboundedPreceding, 0)
    df = df.withColumn("session_seq", F.sum("session_start").over(cum_win))

    # Compose a session_id string
    df = df.withColumn("session_id", F.concat_ws("_", F.col("user_id"), F.col("session_seq")))

    # Prepare per-session aggregation: start, end, duration, count, ordered pages
    # Collect (ts, page_id) then sort within array and extract page_id order
    df_sessions = (
        df.groupBy("user_id", "session_id")
        .agg(
            F.min("ts").alias("start_ts"),
            F.max("ts").alias("end_ts"),
            (F.unix_timestamp(F.max("ts")) - F.unix_timestamp(F.min("ts"))).alias("session_duration_seconds"),
            F.count("*").alias("page_count"),
            F.collect_list(F.struct(F.col("ts"), F.col("page_id"))).alias("pages_struct"),
        )
        .withColumn("pages_sorted", F.expr("array_sort(pages_struct, (left, right) -> case when left.ts < right.ts then -1 when left.ts > right.ts then 1 else 0 end)"))
        .withColumn("pages_visited", F.expr("transform(pages_sorted, x -> x.page_id)"))
        .drop("pages_struct", "pages_sorted")
    )

    # Top pages overall
    top_pages = (
        df.filter(F.col("action") == "page_view")
        .groupBy("page_id")
        .agg(F.count("*").alias("views"))
        .orderBy(F.desc("views"))
    )

    return df_sessions, top_pages


def main():
    parser = argparse.ArgumentParser(description="Sessionize clickstream JSON logs with PySpark")
    parser.add_argument("--input", required=True, help="Input path (JSON files or folder)")
    parser.add_argument("--out_sessions", required=True, help="Output path for session-level parquet")
    parser.add_argument("--out_top", required=True, help="Output path for top-pages parquet/csv")
    parser.add_argument("--session_gap_minutes", type=int, default=30, help="Inactivity threshold to start a new session (minutes)")
    args = parser.parse_args()

    spark = build_spark("clickstream-sessionize")

    sessions_df, top_pages_df = sessionize_logs(spark, args.input, args.session_gap_minutes)

    # Write outputs
    sessions_df.write.mode("overwrite").parquet(args.out_sessions)
    top_pages_df.write.mode("overwrite").parquet(args.out_top)

    spark.stop()


if __name__ == "__main__":
    main()
