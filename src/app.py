"""
Enhanced Streamlit app for interactive clickstream analysis with additional features
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json
import base64
import io


def create_spark_session():
    """Initialize Spark session with appropriate configs"""
    return SparkSession.builder \
        .appName("clickstream-analysis") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def load_and_prepare_data(spark, input_csv):
    """Load and prepare the clickstream data"""
    df = spark.read.option("header", "true").csv(input_csv)
    
    # Add timestamp and convert user_id to string
    df = df.withColumn("ts", F.to_timestamp("Timestamp")) \
           .withColumn("user_id", F.col("UserID").cast("string"))
    
    return df

def analyze_timing_patterns(df, time_unit='hour'):
    """Analyze event timing patterns with flexible time unit"""
    if time_unit == 'hour':
        time_col = F.hour("ts").alias("time_unit")
    elif time_unit == 'day_of_week':
        time_col = F.dayofweek("ts").alias("time_unit")
    else:  # month
        time_col = F.month("ts").alias("time_unit")
    
    # Overall patterns
    time_events = df.groupBy(time_col).count().orderBy("time_unit").toPandas()
    
    # Event type distribution
    event_types_time = df.groupBy(time_col, "EventType") \
        .count() \
        .toPandas()
    
    # Add time unit labels
    if time_unit == 'day_of_week':
        days = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
        time_events['time_label'] = time_events['time_unit'].map(days)
        event_types_time['time_label'] = event_types_time['time_unit'].map(days)
    else:
        time_events['time_label'] = time_events['time_unit']
        event_types_time['time_label'] = event_types_time['time_unit']
    
    return time_events, event_types_time


def analyze_timing_patterns(df):
    """Analyze event timing patterns"""
    # Convert to Pandas for Plotly
    hourly_events = df.groupBy(F.hour("ts").alias("hour")) \
        .count() \
        .orderBy("hour") \
        .toPandas()
    
    event_types_hourly = df.groupBy(
        F.hour("ts").alias("hour"),
        "EventType"
    ).count().toPandas()
    
    return hourly_events, event_types_hourly


def analyze_user_returns(df):
    """Analyze user return patterns"""
    user_window = Window.partitionBy("user_id").orderBy("ts")
    
    user_gaps = df.withColumn(
        "prev_ts",
        F.lag("ts").over(user_window)
    ).withColumn(
        "gap_hours",
        (F.unix_timestamp("ts") - F.unix_timestamp("prev_ts")) / 3600.0
    ).filter(F.col("gap_hours").isNotNull())
    
    return user_gaps.toPandas()


def analyze_product_behavior(df):
    """Analyze product-specific behaviors"""
    product_events = df.filter(F.col("ProductID").isNotNull()) \
        .groupBy("ProductID", "EventType") \
        .count() \
        .toPandas()
    
    product_sequences = df.filter(F.col("ProductID").isNotNull()) \
        .withColumn(
            "next_event",
            F.lead("EventType").over(Window.partitionBy("ProductID").orderBy("ts"))
        ).filter(F.col("next_event").isNotNull()) \
        .groupBy("EventType", "next_event") \
        .count() \
        .toPandas()
    
    return product_events, product_sequences


def main():
    st.set_page_config(
        page_title="E-commerce Clickstream Analysis",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("E-commerce Clickstream Analysis Dashboard")
    
    # Initialize Spark and load data
    spark = create_spark_session()
    input_csv = "data/ecommerce_clickstream_transactions.csv"
    
    with st.spinner("Loading data..."):
        df = load_and_prepare_data(spark, input_csv)
        
    # Sidebar for analysis selection
    analysis_type = st.sidebar.selectbox(
        "Select Analysis Type",
        ["Event Timing Patterns", "User Return Patterns", "Product Behavior", "Overview"]
    )
    
    if analysis_type == "Event Timing Patterns":
        st.header("Event Timing Patterns")
        
        hourly_events, event_types_hourly = analyze_timing_patterns(df)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Hourly Event Distribution")
            fig = px.line(
                hourly_events,
                x="hour",
                y="count",
                title="Events by Hour of Day"
            )
            st.plotly_chart(fig)
        
        with col2:
            st.subheader("Event Types by Hour")
            fig = px.bar(
                event_types_hourly,
                x="hour",
                y="count",
                color="EventType",
                title="Event Types Distribution by Hour"
            )
            st.plotly_chart(fig)
    
    elif analysis_type == "User Return Patterns":
        st.header("User Return Patterns")
        
        user_gaps = analyze_user_returns(df)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Return Time Distribution")
            fig = px.histogram(
                user_gaps,
                x="gap_hours",
                nbins=50,
                title="Distribution of Time Between User Returns (Hours)"
            )
            st.plotly_chart(fig)
        
        with col2:
            st.subheader("Return Pattern Categories")
            categories = pd.cut(
                user_gaps["gap_hours"],
                bins=[0, 1, 24, 168, float('inf')],
                labels=["Same Hour", "Same Day", "Same Week", "Longer"]
            ).value_counts()
            
            fig = px.pie(
                values=categories.values,
                names=categories.index,
                title="User Return Patterns"
            )
            st.plotly_chart(fig)
    
    elif analysis_type == "Product Behavior":
        st.header("Product Behavior Analysis")
        
        product_events, product_sequences = analyze_product_behavior(df)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Product Event Distribution")
            fig = px.bar(
                product_events,
                x="EventType",
                y="count",
                color="EventType",
                title="Events per Product"
            )
            st.plotly_chart(fig)
        
        with col2:
            st.subheader("Event Flow Analysis")
            fig = px.scatter(
                product_sequences,
                x="EventType",
                y="next_event",
                size="count",
                title="Event Flow Patterns"
            )
            st.plotly_chart(fig)
    
    else:  # Overview
        st.header("Dataset Overview")
        
        # Basic stats
        total_events = df.count()
        unique_users = df.select("user_id").distinct().count()
        unique_products = df.select("ProductID").distinct().count()
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Events", f"{total_events:,}")
        with col2:
            st.metric("Unique Users", f"{unique_users:,}")
        with col3:
            st.metric("Unique Products", f"{unique_products:,}")
        
        # Event type distribution
        event_dist = df.groupBy("EventType").count().toPandas()
        
        st.subheader("Event Type Distribution")
        fig = px.pie(
            event_dist,
            values="count",
            names="EventType",
            title="Distribution of Event Types"
        )
        st.plotly_chart(fig)
        
        # Daily activity
        daily_events = df.groupBy(F.date_format("ts", "yyyy-MM-dd").alias("date")) \
            .count() \
            .orderBy("date") \
            .toPandas()
        
        st.subheader("Daily Activity")
        fig = px.line(
            daily_events,
            x="date",
            y="count",
            title="Events per Day"
        )
        st.plotly_chart(fig)


if __name__ == "__main__":

    main()

