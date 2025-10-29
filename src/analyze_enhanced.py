"""
Enhanced clickstream analysis with detailed metrics and session thresholds.
"""
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def analyze_clickstream(spark, input_csv, gap_minutes=30):
    """Process clickstream data with enhanced analytics."""
    print(f"\nAnalyzing with session gap threshold: {gap_minutes} minutes")
    print(f"Reading {input_csv}...")
    
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
    
    # Basic stats with user engagement levels
    print("\n=== Basic Statistics and User Engagement ===")
    user_stats = df.groupBy("user_id").agg(
        F.count("*").alias("total_events"),
        F.countDistinct("session_id").alias("total_sessions"),
        F.sum(F.when(F.col("EventType") == "purchase", 1).otherwise(0)).alias("purchases"),
        F.sum(F.when(F.col("EventType") == "add_to_cart", 1).otherwise(0)).alias("cart_adds")
    )
    
    engagement_stats = user_stats.select(
        F.count("user_id").alias("total_users"),
        F.avg("total_events").alias("avg_events_per_user"),
        F.avg("total_sessions").alias("avg_sessions_per_user"),
        F.count(F.when(F.col("purchases") > 0, True)).alias("users_with_purchase"),
        F.count(F.when(F.col("cart_adds") > 0, True)).alias("users_with_cart_adds")
    ).collect()[0]
    
    print(f"Total users: {engagement_stats['total_users']:,}")
    print(f"Average events per user: {engagement_stats['avg_events_per_user']:.2f}")
    print(f"Average sessions per user: {engagement_stats['avg_sessions_per_user']:.2f}")
    print(f"Users who made purchases: {engagement_stats['users_with_purchase']:,}")
    print(f"Users who added to cart: {engagement_stats['users_with_cart_adds']:,}")
    
    # Event type distribution with session context
    print("\n=== Event Type Distribution by Session Position ===")
    df.withColumn(
        "event_position",
        F.row_number().over(Window.partitionBy("session_id").orderBy("ts"))
    ).groupBy("EventType", "event_position") \
        .count() \
        .orderBy("event_position", F.desc("count")) \
        .show()
    
    # Session statistics with duration buckets
    print("\n=== Session Duration Distribution ===")
    session_stats = df.groupBy("session_id") \
        .agg(
            F.min("ts").alias("start_ts"),
            F.max("ts").alias("end_ts"),
            F.round(
                (F.unix_timestamp(F.max("ts")) - F.unix_timestamp(F.min("ts"))) / 60.0,
                2
            ).alias("duration_minutes"),
            F.count("*").alias("events"),
            F.collect_list("EventType").alias("event_sequence")
        )
    
    # Duration buckets
    session_stats.select(
        F.when(F.col("duration_minutes") == 0, "Instant")
         .when(F.col("duration_minutes") <= 1, "≤ 1 min")
         .when(F.col("duration_minutes") <= 5, "1-5 mins")
         .when(F.col("duration_minutes") <= 15, "5-15 mins")
         .otherwise("> 15 mins").alias("duration_bucket")
    ).groupBy("duration_bucket") \
        .count() \
        .orderBy("duration_bucket") \
        .show()
    
    # Conversion analysis
    print("\n=== Conversion Funnel Analysis ===")
    funnel_stats = df.groupBy("session_id").agg(
        F.max(F.when(F.col("EventType") == "product_view", 1).otherwise(0)).alias("reached_product"),
        F.max(F.when(F.col("EventType") == "add_to_cart", 1).otherwise(0)).alias("reached_cart"),
        F.max(F.when(F.col("EventType") == "purchase", 1).otherwise(0)).alias("reached_purchase")
    )
    
    conversions = funnel_stats.agg(
        F.count("session_id").alias("total_sessions"),
        F.sum("reached_product").alias("product_views"),
        F.sum("reached_cart").alias("cart_adds"),
        F.sum("reached_purchase").alias("purchases")
    ).collect()[0]
    
    total = float(conversions["total_sessions"])
    print(f"Product View Rate: {(conversions['product_views']/total)*100:.1f}%")
    print(f"Add to Cart Rate: {(conversions['cart_adds']/total)*100:.1f}%")
    print(f"Purchase Rate: {(conversions['purchases']/total)*100:.1f}%")
    
    # Common event sequences
    print("\n=== Top 10 Event Sequences ===")
    session_stats.select(
        F.concat_ws(" → ", "event_sequence").alias("sequence"),
        F.size("event_sequence").alias("steps")
    ).filter(F.col("steps") > 1) \
        .groupBy("sequence") \
        .count() \
        .orderBy(F.desc("count")) \
        .limit(10) \
        .show(truncate=False)


def main():
    parser = argparse.ArgumentParser(
        description="Enhanced clickstream analysis with PySpark"
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