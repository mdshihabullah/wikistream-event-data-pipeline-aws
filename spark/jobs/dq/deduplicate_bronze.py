#!/usr/bin/env python3
"""
One-time Bronze Layer Deduplication Script
===========================================
Removes duplicate event_ids from bronze.raw_events table.

This script:
1. Identifies duplicate event_ids in the bronze layer
2. Keeps the earliest record (by bronze_processed_at)
3. Deletes duplicate records using Iceberg DELETE

Run this once to clean up existing duplicates, then the streaming job's
stateful deduplication will prevent future duplicates.

Usage:
    spark-submit \
        --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
        --conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:us-east-1:160884803380:bucket/wikistream-dev-tables \
        deduplicate_bronze.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, count
from pyspark.sql.window import Window
from datetime import datetime


def create_spark_session() -> SparkSession:
    """Create SparkSession for EMR Serverless with S3 Tables Catalog."""
    return (
        SparkSession.builder
        .appName("WikiStream-Bronze-Deduplication")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .getOrCreate()
    )


def main():
    print("=" * 60)
    print("WikiStream Bronze Deduplication")
    print(f"Started at: {datetime.utcnow().isoformat()}")
    print("=" * 60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Step 1: Identify duplicates
    print("\n[1/4] Identifying duplicate event_ids...")
    
    bronze_df = spark.sql("SELECT * FROM s3tablesbucket.bronze.raw_events")
    total_records = bronze_df.count()
    print(f"Total records: {total_records:,}")
    
    # Find event_ids with duplicates
    duplicate_ids = (
        bronze_df
        .groupBy("event_id")
        .agg(count("*").alias("count"))
        .filter(col("count") > 1)
    )
    
    duplicate_count = duplicate_ids.count()
    total_duplicates = duplicate_ids.agg({"count": "sum"}).collect()[0][0] - duplicate_count
    
    print(f"Unique event_ids with duplicates: {duplicate_count:,}")
    print(f"Total duplicate records to remove: {total_duplicates:,}")
    print(f"Uniqueness rate: {((total_records - total_duplicates) / total_records * 100):.2f}%")
    
    if duplicate_count == 0:
        print("\n✅ No duplicates found - bronze layer is clean!")
        spark.stop()
        return
    
    # Step 2: Identify records to keep (earliest by bronze_processed_at)
    print("\n[2/4] Identifying records to keep (earliest bronze_processed_at)...")
    
    window_spec = Window.partitionBy("event_id").orderBy(col("bronze_processed_at").asc())
    
    records_with_rank = bronze_df.withColumn("rank", row_number().over(window_spec))
    
    # Records to delete (rank > 1)
    records_to_delete = records_with_rank.filter(col("rank") > 1)
    delete_count = records_to_delete.count()
    
    print(f"Records to delete: {delete_count:,}")
    
    # Step 3: Create temp view for DELETE operation
    print("\n[3/4] Preparing DELETE operation...")
    
    records_to_delete.select("event_id", "bronze_processed_at").createOrReplaceTempView("duplicates_to_delete")
    
    # Step 4: Execute DELETE using Iceberg
    print("\n[4/4] Executing DELETE operation...")
    print("⚠️  This may take several minutes for large datasets...")
    
    # Iceberg DELETE: Remove duplicates keeping only the earliest record
    spark.sql("""
        DELETE FROM s3tablesbucket.bronze.raw_events
        WHERE event_id IN (
            SELECT DISTINCT event_id FROM duplicates_to_delete
        )
        AND bronze_processed_at NOT IN (
            SELECT MIN(bronze_processed_at) 
            FROM s3tablesbucket.bronze.raw_events 
            GROUP BY event_id
        )
    """)
    
    print("✅ DELETE operation completed")
    
    # Step 5: Verify cleanup
    print("\n[5/5] Verifying cleanup...")
    
    final_df = spark.sql("SELECT * FROM s3tablesbucket.bronze.raw_events")
    final_count = final_df.count()
    final_unique = final_df.select("event_id").distinct().count()
    
    print(f"Final record count: {final_count:,}")
    print(f"Unique event_ids: {final_unique:,}")
    print(f"Uniqueness rate: {(final_unique / final_count * 100):.2f}%")
    
    if final_unique == final_count:
        print("\n✅ SUCCESS: Bronze layer is now 100% unique!")
    else:
        print(f"\n⚠️  WARNING: Still have {final_count - final_unique:,} duplicates")
    
    print("\n" + "=" * 60)
    print("Deduplication Complete")
    print(f"Completed at: {datetime.utcnow().isoformat()}")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    main()
