"""
Silver Layer Schema Definition
==============================
Cleaned, deduplicated, and enriched event schemas.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    BooleanType,
    TimestampType,
    DoubleType,
)


# =============================================================================
# SILVER CLEANED EVENTS TABLE SCHEMA
# =============================================================================

SILVER_EVENTS_SCHEMA = StructType([
    # ===== Primary Key =====
    StructField("event_id", StringType(), False),
    StructField("rc_id", LongType(), False),
    
    # ===== Event Classification =====
    StructField("event_type", StringType(), False),
    StructField("event_subtype", StringType(), True),  # Derived: log_type for logs
    StructField("namespace", IntegerType(), True),
    StructField("namespace_name", StringType(), True),  # Enriched: Main, Talk, User, etc.
    
    # ===== Domain & Geography =====
    StructField("domain", StringType(), False),
    StructField("wiki", StringType(), True),
    StructField("language", StringType(), True),      # Extracted from domain
    StructField("region", StringType(), True),        # Mapped from REGIONAL_DOMAINS
    
    # ===== Content (Cleaned) =====
    StructField("title", StringType(), True),
    StructField("title_normalized", StringType(), True),  # Lowercase, trimmed
    StructField("comment", StringType(), True),
    StructField("comment_length", IntegerType(), True),
    
    # ===== User Information (Cleaned) =====
    StructField("user", StringType(), True),
    StructField("user_normalized", StringType(), True),  # Lowercase, trimmed
    StructField("is_bot", BooleanType(), False),
    StructField("is_anonymous", BooleanType(), False),   # Derived: IP-based username
    
    # ===== Change Metrics =====
    StructField("is_minor", BooleanType(), True),
    StructField("is_patrolled", BooleanType(), True),
    StructField("length_old", IntegerType(), True),
    StructField("length_new", IntegerType(), True),
    StructField("length_delta", IntegerType(), True),
    StructField("length_delta_abs", IntegerType(), True),
    StructField("is_content_addition", BooleanType(), True),  # delta > 0
    StructField("is_content_removal", BooleanType(), True),   # delta < 0
    
    # ===== Revision =====
    StructField("revision_old", LongType(), True),
    StructField("revision_new", LongType(), True),
    
    # ===== Timestamps =====
    StructField("event_timestamp", TimestampType(), False),
    StructField("event_date", StringType(), False),
    StructField("event_hour", IntegerType(), False),
    StructField("event_minute", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),  # 1=Monday, 7=Sunday
    StructField("is_weekend", BooleanType(), True),
    
    # ===== Quality Flags =====
    StructField("is_valid", BooleanType(), False),
    StructField("quality_flags", StringType(), True),  # JSON array of issues
    
    # ===== Processing Metadata =====
    StructField("processed_at", TimestampType(), False),
    StructField("processing_version", StringType(), True),
])


# =============================================================================
# SILVER USER ACTIVITY TABLE SCHEMA
# =============================================================================

SILVER_USER_ACTIVITY_SCHEMA = StructType([
    # ===== Composite Key =====
    StructField("user", StringType(), False),
    StructField("domain", StringType(), False),
    StructField("activity_date", StringType(), False),
    StructField("activity_hour", IntegerType(), False),
    
    # ===== User Attributes =====
    StructField("is_bot", BooleanType(), False),
    StructField("is_anonymous", BooleanType(), False),
    
    # ===== Hourly Aggregates =====
    StructField("edit_count", LongType(), False),
    StructField("new_page_count", LongType(), False),
    StructField("log_action_count", LongType(), False),
    StructField("categorize_count", LongType(), False),
    StructField("total_action_count", LongType(), False),
    
    # ===== Edit Metrics =====
    StructField("minor_edit_count", LongType(), True),
    StructField("total_bytes_added", LongType(), True),
    StructField("total_bytes_removed", LongType(), True),
    StructField("net_bytes_change", LongType(), True),
    
    # ===== Namespace Distribution =====
    StructField("main_namespace_edits", LongType(), True),
    StructField("talk_namespace_edits", LongType(), True),
    StructField("user_namespace_edits", LongType(), True),
    StructField("other_namespace_edits", LongType(), True),
    
    # ===== Distinct Counts =====
    StructField("unique_pages_edited", LongType(), True),
    
    # ===== Timestamps =====
    StructField("first_activity", TimestampType(), True),
    StructField("last_activity", TimestampType(), True),
    
    # ===== Processing Metadata =====
    StructField("processed_at", TimestampType(), False),
])


# =============================================================================
# NAMESPACE MAPPING
# =============================================================================

NAMESPACE_MAPPING = {
    0: "Main",
    1: "Talk",
    2: "User",
    3: "User talk",
    4: "Wikipedia",
    5: "Wikipedia talk",
    6: "File",
    7: "File talk",
    8: "MediaWiki",
    9: "MediaWiki talk",
    10: "Template",
    11: "Template talk",
    12: "Help",
    13: "Help talk",
    14: "Category",
    15: "Category talk",
    100: "Portal",
    101: "Portal talk",
    118: "Draft",
    119: "Draft talk",
    828: "Module",
    829: "Module talk",
}


# =============================================================================
# SILVER TABLE DDLs
# =============================================================================

SILVER_EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.cleaned_events (
    event_id STRING NOT NULL,
    rc_id BIGINT NOT NULL,
    event_type STRING NOT NULL,
    event_subtype STRING,
    namespace INT,
    namespace_name STRING,
    domain STRING NOT NULL,
    wiki STRING,
    language STRING,
    region STRING,
    title STRING,
    title_normalized STRING,
    comment STRING,
    comment_length INT,
    user STRING,
    user_normalized STRING,
    is_bot BOOLEAN NOT NULL,
    is_anonymous BOOLEAN NOT NULL,
    is_minor BOOLEAN,
    is_patrolled BOOLEAN,
    length_old INT,
    length_new INT,
    length_delta INT,
    length_delta_abs INT,
    is_content_addition BOOLEAN,
    is_content_removal BOOLEAN,
    revision_old BIGINT,
    revision_new BIGINT,
    event_timestamp TIMESTAMP NOT NULL,
    event_hour INT NOT NULL,
    event_minute INT,
    day_of_week INT,
    is_weekend BOOLEAN,
    is_valid BOOLEAN NOT NULL,
    quality_flags STRING,
    processed_at TIMESTAMP NOT NULL,
    processing_version STRING
)
USING iceberg
PARTITIONED BY (event_date STRING, domain STRING)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.distribution-mode' = 'hash',
    'write.merge.mode' = 'merge-on-read',
    'format-version' = '2'
)
"""


SILVER_USER_ACTIVITY_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.user_activity (
    user STRING NOT NULL,
    domain STRING NOT NULL,
    activity_hour INT NOT NULL,
    is_bot BOOLEAN NOT NULL,
    is_anonymous BOOLEAN NOT NULL,
    edit_count BIGINT NOT NULL,
    new_page_count BIGINT NOT NULL,
    log_action_count BIGINT NOT NULL,
    categorize_count BIGINT NOT NULL,
    total_action_count BIGINT NOT NULL,
    minor_edit_count BIGINT,
    total_bytes_added BIGINT,
    total_bytes_removed BIGINT,
    net_bytes_change BIGINT,
    main_namespace_edits BIGINT,
    talk_namespace_edits BIGINT,
    user_namespace_edits BIGINT,
    other_namespace_edits BIGINT,
    unique_pages_edited BIGINT,
    first_activity TIMESTAMP,
    last_activity TIMESTAMP,
    processed_at TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (activity_date STRING)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'format-version' = '2'
)
"""



