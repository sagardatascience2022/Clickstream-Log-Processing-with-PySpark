"""
Process e-commerce clickstream data from CSV format using PySpark.
Handles the structure: UserID,SessionID,Timestamp,EventType,ProductID,Amount,Outcome
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


def build_spark(app_name="clickstream-analysis"):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def process_clickstream(spark, input_path, session_gap_minutes=30):
    # Read CSV with header
    df = spark.read.option("header", "true").csv(input_path)

    # Ensure expected columns exist
    expected = ["UserID", "Timestamp", "EventType", "ProductID"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Input CSV missing expected columns: {missing}")

    # Convert timestamp string to TimestampType and UserID to string
    df = df.withColumn("ts", F.to_timestamp("Timestamp")) \
           .withColumn("user_id", F.col("UserID").cast("string"))

    # Window specs for time-based analysis
    user_time_window = Window.partitionBy("user_id").orderBy("ts")

    # Compute time gap between events
    df = df.withColumn(
        "prev_ts",
        F.lag("ts").over(user_time_window)
    ).withColumn(
        "gap_minutes",
        (F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")) / 60.0
    )

    # Flag new sessions (first event or gap > threshold)
    df = df.withColumn(
        "new_session",
        F.when(
            F.col("prev_ts").isNull() | (F.col("gap_minutes") > session_gap_minutes),
            1
        ).otherwise(0)
    )

    # Generate session IDs using cumulative sum of new_session flag
    df = df.withColumn(
        "session_seq",
        F.sum("new_session").over(user_time_window)
    ).withColumn(
        "session_id",
        F.concat_ws("_", F.col("user_id"), F.col("session_seq"))
    )

    # Compute session-level metrics
    sessions_df = df.groupBy("user_id", "session_id").agg(
        F.min("ts").alias("start_ts"),
        F.max("ts").alias("end_ts"),
        F.round(
            (F.unix_timestamp(F.max("ts")) - F.unix_timestamp(F.min("ts"))) / 60.0,
            2
        ).alias("session_duration_minutes"),
        F.count("*").alias("event_count"),
        F.count_distinct("ProductID").alias("unique_products_viewed"),
        F.sum(F.when(F.col("EventType") == "add_to_cart", 1).otherwise(0)).alias("items_added_to_cart"),
        F.collect_list(
            F.when(F.col("EventType") == "product_view", F.col("ProductID"))
            .otherwise(None)
        ).alias("products_viewed")
    )

    # Get product view statistics
    product_views = (
        df.filter(F.col("EventType") == "product_view")
        .filter(F.col("ProductID").isNotNull())
        .groupBy("ProductID")
        .agg(
            F.count("*").alias("view_count"),
            F.countDistinct("user_id").alias("unique_viewers"),
            F.sum(
                F.when(F.col("EventType") == "add_to_cart", 1).otherwise(0)
            ).alias("cart_adds")
        )
        .orderBy(F.desc("view_count"))
    )

    return sessions_df, product_views


def main():
    parser = argparse.ArgumentParser(description="Analyze e-commerce clickstream data with PySpark")
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--out_sessions", required=True, help="Output path for session-level parquet")
    parser.add_argument("--out_products", required=True, help="Output path for product-level parquet")
    parser.add_argument("--session_gap_minutes", type=int, default=30,
                      help="Minutes of inactivity to start new session")
    args = parser.parse_args()

    # Initialize Spark
    spark = build_spark()

    try:
        # Process clickstream data
        sessions_df, product_views_df = process_clickstream(
            spark, args.input, args.session_gap_minutes
        )

        # Write outputs
        sessions_df.write.mode("overwrite").parquet(args.out_sessions)
        product_views_df.write.mode("overwrite").parquet(args.out_products)

        # Show quick summary
        print("\nSession Statistics:")
        sessions_df.select(
            F.count("*").alias("total_sessions"),
            F.approx_count_distinct("user_id").alias("unique_users"),
            F.round(F.avg("session_duration_minutes"), 2).alias("avg_session_minutes"),
            F.round(F.avg("event_count"), 2).alias("avg_events_per_session")
        ).show()

        print("\nTop 5 Products by Views:")
        product_views_df.select(
            "ProductID", "view_count", "unique_viewers", "cart_adds"
        ).limit(5).show()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()