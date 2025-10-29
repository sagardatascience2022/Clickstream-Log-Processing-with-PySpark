"""
Simple script to process clickstream data and write CSV outputs.
Avoids Hadoop dependencies by using CSV instead of Parquet.
"""
import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def analyze_clickstream(input_csv, out_dir, gap_minutes=30):
    """Process clickstream data and write results as CSV."""
    # Create Spark session
    spark = SparkSession.builder \
        .appName("clickstream-analysis") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    try:
        # Read input CSV
        print(f"Reading {input_csv}...")
        df = spark.read.option("header", "true").csv(input_csv)
        
        # Show sample of data
        print("\nInput schema and sample:")
        df.printSchema()
        df.show(5)
        
        # Basic counts
        total_events = df.count()
        unique_users = df.select("UserID").distinct().count()
        print(f"\nTotal events: {total_events:,}")
        print(f"Unique users: {unique_users:,}")
        
        # Top pages/products
        top_products = df.filter(df.EventType == "product_view") \
            .groupBy("ProductID") \
            .agg(
                F.count("*").alias("views"),
                F.countDistinct("UserID").alias("unique_viewers")
            ) \
            .orderBy(F.desc("views"))
        
        print("\nTop 10 products by views:")
        top_products.show(10)
        
        # Ensure output directory exists
        os.makedirs(out_dir, exist_ok=True)
        
        # Write top products to CSV
        products_path = os.path.join(out_dir, "top_products")
        print(f"\nWriting product stats to: {products_path}")
        top_products.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .csv(products_path)
            
        print("Analysis complete!")
        
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze clickstream data and output CSVs"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input CSV file with clickstream data"
    )
    parser.add_argument(
        "--outdir",
        required=True,
        help="Output directory for analysis results"
    )
    parser.add_argument(
        "--gap-minutes",
        type=int,
        default=30,
        help="Session gap threshold in minutes (default: 30)"
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input):
        print(f"Error: Input file not found: {args.input}")
        sys.exit(1)
        
    analyze_clickstream(args.input, args.outdir, args.gap_minutes)


if __name__ == "__main__":
    main()