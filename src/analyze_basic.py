"""
Analyze clickstream data and show results (avoiding file writes).
"""
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def analyze_clickstream(spark, input_csv, gap_minutes=30):
    """Process clickstream data and show analytics."""
    print(f"\nReading {input_csv}...")
    df = spark.read.option("header", "true").csv(input_csv)
    
    # Add timestamp and convert user_id to string
    df = df.withColumn("ts", F.to_timestamp("Timestamp")) \
           .withColumn("user_id", F.col("UserID").cast("string"))
    
    # Window for user activity
    user_window = Window.partitionBy("user_id").orderBy("ts")
    
    # Compute time gaps between events
    df = df.withColumn(
        "prev_ts",
        F.lag("ts").over(user_window)
    ).withColumn(
        "gap_minutes",
        (F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")) / 60.0
    )
    
    # Flag new sessions
    df = df.withColumn(
        "new_session",
        F.when(
            F.col("prev_ts").isNull() | (F.col("gap_minutes") > gap_minutes),
            1
        ).otherwise(0)
    )
    
    # Generate session IDs
    df = df.withColumn(
        "session_seq",
        F.sum("new_session").over(user_window)
    ).withColumn(
        "session_id",
        F.concat_ws("_", F.col("user_id"), F.col("session_seq"))
    )
    
    # Basic stats
    print("\n=== Basic Statistics ===")
    basic_stats = df.agg(
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("session_id").alias("total_sessions"),
        F.count("*").alias("total_events")
    ).collect()[0]
    
    print(f"Unique users: {basic_stats['unique_users']:,}")
    print(f"Total sessions: {basic_stats['total_sessions']:,}")
    print(f"Total events: {basic_stats['total_events']:,}")
    
    # Event type distribution
    print("\n=== Event Type Distribution ===")
    df.groupBy("EventType") \
        .agg(F.count("*").alias("count")) \
        .orderBy(F.desc("count")) \
        .show()
    
    # Session statistics
    print("\n=== Session Statistics ===")
    session_stats = df.groupBy("session_id") \
        .agg(
            F.min("ts").alias("start_ts"),
            F.max("ts").alias("end_ts"),
            F.round(
                (F.unix_timestamp(F.max("ts")) - F.unix_timestamp(F.min("ts"))) / 60.0,
                2
            ).alias("duration_minutes"),
            F.count("*").alias("events"),
            F.sum(F.when(F.col("EventType") == "add_to_cart", 1).otherwise(0)).alias("cart_adds")
        )
    
    session_stats.select(
        F.round(F.avg("duration_minutes"), 2).alias("avg_duration_min"),
        F.round(F.avg("events"), 2).alias("avg_events"),
        F.round(F.avg("cart_adds"), 2).alias("avg_cart_adds")
    ).show()
    
    # Top products
    print("\n=== Top 10 Products by Views ===")
    df.filter(F.col("EventType") == "product_view") \
        .filter(F.col("ProductID").isNotNull()) \
        .groupBy("ProductID") \
        .agg(
            F.count("*").alias("views"),
            F.countDistinct("user_id").alias("unique_viewers"),
            F.sum(
                F.when(F.col("EventType") == "add_to_cart", 1).otherwise(0)
            ).alias("cart_adds")
        ) \
        .orderBy(F.desc("views")) \
        .limit(10) \
        .show()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze clickstream data with PySpark"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input CSV file with clickstream data"
    )
    parser.add_argument(
        "--gap-minutes",
        type=int,
        default=30,
        help="Session gap threshold in minutes (default: 30)"
    )
    
    args = parser.parse_args()
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("clickstream-analysis") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    try:
        analyze_clickstream(spark, args.input, args.gap_minutes)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()