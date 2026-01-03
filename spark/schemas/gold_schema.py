"""
Gold Layer Schema Definition
============================
Aggregated, business-ready analytics tables for dashboarding.
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
    ArrayType,
    MapType,
)


# =============================================================================
# GOLD HOURLY STATISTICS TABLE
# =============================================================================

GOLD_HOURLY_STATS_SCHEMA = StructType([
    # ===== Time Dimensions =====
    StructField("stat_date", StringType(), False),
    StructField("stat_hour", IntegerType(), False),
    StructField("domain", StringType(), False),
    StructField("region", StringType(), True),
    
    # ===== Volume Metrics =====
    StructField("total_events", LongType(), False),
    StructField("edit_count", LongType(), False),
    StructField("new_page_count", LongType(), False),
    StructField("log_action_count", LongType(), False),
    StructField("categorize_count", LongType(), False),
    
    # ===== User Metrics =====
    StructField("unique_users", LongType(), False),
    StructField("unique_bots", LongType(), False),
    StructField("unique_humans", LongType(), False),
    StructField("anonymous_edits", LongType(), False),
    StructField("registered_edits", LongType(), False),
    
    # ===== Content Metrics =====
    StructField("bytes_added", LongType(), True),
    StructField("bytes_removed", LongType(), True),
    StructField("net_bytes_change", LongType(), True),
    StructField("unique_pages_modified", LongType(), False),
    StructField("minor_edits", LongType(), True),
    
    # ===== Namespace Distribution =====
    StructField("main_namespace_events", LongType(), True),
    StructField("talk_namespace_events", LongType(), True),
    StructField("user_namespace_events", LongType(), True),
    StructField("file_namespace_events", LongType(), True),
    StructField("category_namespace_events", LongType(), True),
    StructField("template_namespace_events", LongType(), True),
    
    # ===== Derived Metrics =====
    StructField("bot_percentage", DoubleType(), True),
    StructField("avg_edit_size", DoubleType(), True),
    StructField("edits_per_user", DoubleType(), True),
    
    # ===== Hourly Trends (vs previous hour) =====
    StructField("events_change_pct", DoubleType(), True),
    StructField("users_change_pct", DoubleType(), True),
    
    # ===== Processing Metadata =====
    StructField("processed_at", TimestampType(), False),
])


# =============================================================================
# GOLD USER METRICS TABLE (Daily Summary)
# =============================================================================

GOLD_USER_METRICS_SCHEMA = StructType([
    # ===== Dimensions =====
    StructField("stat_date", StringType(), False),
    StructField("user", StringType(), False),
    StructField("user_type", StringType(), False),  # bot, anonymous, registered
    
    # ===== Activity Metrics =====
    StructField("total_actions", LongType(), False),
    StructField("edits", LongType(), False),
    StructField("new_pages", LongType(), False),
    StructField("log_actions", LongType(), False),
    
    # ===== Domain Activity =====
    StructField("domains_active", LongType(), False),
    StructField("primary_domain", StringType(), True),  # Most active domain
    StructField("domain_distribution", MapType(StringType(), LongType()), True),
    
    # ===== Content Contribution =====
    StructField("total_bytes_contributed", LongType(), True),
    StructField("net_bytes_change", LongType(), True),
    StructField("unique_pages_edited", LongType(), False),
    StructField("minor_edit_ratio", DoubleType(), True),
    
    # ===== Activity Pattern =====
    StructField("active_hours", LongType(), True),  # Number of distinct hours
    StructField("peak_hour", IntegerType(), True),  # Hour with most activity
    StructField("first_action_time", TimestampType(), True),
    StructField("last_action_time", TimestampType(), True),
    StructField("session_duration_minutes", LongType(), True),
    
    # ===== Historical Comparison =====
    StructField("actions_7d_avg", DoubleType(), True),
    StructField("actions_30d_avg", DoubleType(), True),
    StructField("is_new_user_today", BooleanType(), True),
    
    # ===== Risk Indicators =====
    StructField("rapid_edit_flag", BooleanType(), True),  # >100 edits/hour
    StructField("large_deletion_flag", BooleanType(), True),  # Single edit removing >10KB
    StructField("cross_domain_flag", BooleanType(), True),  # Active in >5 domains
    
    # ===== Processing Metadata =====
    StructField("processed_at", TimestampType(), False),
])


# =============================================================================
# GOLD RISK SCORES TABLE
# =============================================================================

GOLD_RISK_SCORES_SCHEMA = StructType([
    # ===== Identification =====
    StructField("stat_date", StringType(), False),
    StructField("stat_hour", IntegerType(), False),
    StructField("entity_type", StringType(), False),  # user, domain, page
    StructField("entity_id", StringType(), False),    # user name, domain, page title
    
    # ===== Risk Score =====
    StructField("risk_score", DoubleType(), False),   # 0.0 - 1.0
    StructField("risk_level", StringType(), False),   # low, medium, high, critical
    
    # ===== Risk Factors (Evidence Fields) =====
    StructField("velocity_score", DoubleType(), True),  # Abnormal edit velocity
    StructField("deletion_score", DoubleType(), True),  # Large content deletions
    StructField("revert_score", DoubleType(), True),    # Reverted edits ratio
    StructField("cross_wiki_score", DoubleType(), True),  # Cross-wiki activity
    StructField("new_user_score", DoubleType(), True),   # New user risk
    StructField("anonymous_score", DoubleType(), True),  # Anonymous edit risk
    
    # ===== Evidence =====
    StructField("evidence", ArrayType(StructType([
        StructField("factor", StringType(), True),
        StructField("value", StringType(), True),
        StructField("threshold", StringType(), True),
        StructField("contribution", DoubleType(), True),
    ])), True),
    
    # ===== Context =====
    StructField("recent_actions_count", LongType(), True),
    StructField("affected_pages", ArrayType(StringType()), True),
    StructField("sample_comments", ArrayType(StringType()), True),
    
    # ===== Alert Status =====
    StructField("alert_triggered", BooleanType(), False),
    StructField("alert_id", StringType(), True),
    StructField("alert_timestamp", TimestampType(), True),
    
    # ===== Processing Metadata =====
    StructField("processed_at", TimestampType(), False),
])


# =============================================================================
# GOLD DOMAIN TRENDS TABLE
# =============================================================================

GOLD_DOMAIN_TRENDS_SCHEMA = StructType([
    # ===== Dimensions =====
    StructField("stat_date", StringType(), False),
    StructField("domain", StringType(), False),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    
    # ===== Daily Summary =====
    StructField("total_events", LongType(), False),
    StructField("total_edits", LongType(), False),
    StructField("total_new_pages", LongType(), False),
    StructField("unique_users", LongType(), False),
    StructField("unique_pages", LongType(), False),
    
    # ===== Content Metrics =====
    StructField("total_bytes_added", LongType(), True),
    StructField("total_bytes_removed", LongType(), True),
    StructField("net_content_change", LongType(), True),
    StructField("avg_edit_size", DoubleType(), True),
    
    # ===== User Distribution =====
    StructField("bot_events", LongType(), True),
    StructField("bot_percentage", DoubleType(), True),
    StructField("anonymous_events", LongType(), True),
    StructField("anonymous_percentage", DoubleType(), True),
    StructField("top_contributors", ArrayType(StructType([
        StructField("user", StringType(), True),
        StructField("edits", LongType(), True),
    ])), True),
    
    # ===== Temporal Pattern =====
    StructField("peak_hour", IntegerType(), True),
    StructField("peak_hour_events", LongType(), True),
    StructField("hourly_distribution", MapType(IntegerType(), LongType()), True),
    
    # ===== Trending Content =====
    StructField("most_edited_pages", ArrayType(StructType([
        StructField("title", StringType(), True),
        StructField("edit_count", LongType(), True),
    ])), True),
    
    # ===== Trend Comparison =====
    StructField("events_1d_ago", LongType(), True),
    StructField("events_7d_ago", LongType(), True),
    StructField("events_30d_ago", LongType(), True),
    StructField("day_over_day_change", DoubleType(), True),
    StructField("week_over_week_change", DoubleType(), True),
    
    # ===== Anomaly Detection =====
    StructField("is_anomalous_volume", BooleanType(), True),
    StructField("anomaly_score", DoubleType(), True),
    
    # ===== Processing Metadata =====
    StructField("processed_at", TimestampType(), False),
])


# =============================================================================
# GOLD TABLE DDLs
# =============================================================================

GOLD_HOURLY_STATS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.hourly_statistics (
    stat_hour INT NOT NULL,
    domain STRING NOT NULL,
    region STRING,
    total_events BIGINT NOT NULL,
    edit_count BIGINT NOT NULL,
    new_page_count BIGINT NOT NULL,
    log_action_count BIGINT NOT NULL,
    categorize_count BIGINT NOT NULL,
    unique_users BIGINT NOT NULL,
    unique_bots BIGINT NOT NULL,
    unique_humans BIGINT NOT NULL,
    anonymous_edits BIGINT NOT NULL,
    registered_edits BIGINT NOT NULL,
    bytes_added BIGINT,
    bytes_removed BIGINT,
    net_bytes_change BIGINT,
    unique_pages_modified BIGINT NOT NULL,
    minor_edits BIGINT,
    main_namespace_events BIGINT,
    talk_namespace_events BIGINT,
    user_namespace_events BIGINT,
    file_namespace_events BIGINT,
    category_namespace_events BIGINT,
    template_namespace_events BIGINT,
    bot_percentage DOUBLE,
    avg_edit_size DOUBLE,
    edits_per_user DOUBLE,
    events_change_pct DOUBLE,
    users_change_pct DOUBLE,
    processed_at TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (stat_date STRING)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'format-version' = '2'
)
"""

GOLD_RISK_SCORES_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.risk_scores (
    stat_hour INT NOT NULL,
    entity_type STRING NOT NULL,
    entity_id STRING NOT NULL,
    risk_score DOUBLE NOT NULL,
    risk_level STRING NOT NULL,
    velocity_score DOUBLE,
    deletion_score DOUBLE,
    revert_score DOUBLE,
    cross_wiki_score DOUBLE,
    new_user_score DOUBLE,
    anonymous_score DOUBLE,
    evidence STRING,
    recent_actions_count BIGINT,
    affected_pages STRING,
    sample_comments STRING,
    alert_triggered BOOLEAN NOT NULL,
    alert_id STRING,
    alert_timestamp TIMESTAMP,
    processed_at TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (stat_date STRING)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'format-version' = '2'
)
"""

GOLD_DOMAIN_TRENDS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.domain_trends (
    domain STRING NOT NULL,
    region STRING,
    language STRING,
    total_events BIGINT NOT NULL,
    total_edits BIGINT NOT NULL,
    total_new_pages BIGINT NOT NULL,
    unique_users BIGINT NOT NULL,
    unique_pages BIGINT NOT NULL,
    total_bytes_added BIGINT,
    total_bytes_removed BIGINT,
    net_content_change BIGINT,
    avg_edit_size DOUBLE,
    bot_events BIGINT,
    bot_percentage DOUBLE,
    anonymous_events BIGINT,
    anonymous_percentage DOUBLE,
    top_contributors STRING,
    peak_hour INT,
    peak_hour_events BIGINT,
    hourly_distribution STRING,
    most_edited_pages STRING,
    events_1d_ago BIGINT,
    events_7d_ago BIGINT,
    events_30d_ago BIGINT,
    day_over_day_change DOUBLE,
    week_over_week_change DOUBLE,
    is_anomalous_volume BOOLEAN,
    anomaly_score DOUBLE,
    processed_at TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (stat_date STRING)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'format-version' = '2'
)
"""

GOLD_USER_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{namespace}.user_metrics (
    user STRING NOT NULL,
    user_type STRING NOT NULL,
    total_actions BIGINT NOT NULL,
    edits BIGINT NOT NULL,
    new_pages BIGINT NOT NULL,
    log_actions BIGINT NOT NULL,
    domains_active BIGINT NOT NULL,
    primary_domain STRING,
    domain_distribution STRING,
    total_bytes_contributed BIGINT,
    net_bytes_change BIGINT,
    unique_pages_edited BIGINT NOT NULL,
    minor_edit_ratio DOUBLE,
    active_hours BIGINT,
    peak_hour INT,
    first_action_time TIMESTAMP,
    last_action_time TIMESTAMP,
    session_duration_minutes BIGINT,
    actions_7d_avg DOUBLE,
    actions_30d_avg DOUBLE,
    is_new_user_today BOOLEAN,
    rapid_edit_flag BOOLEAN,
    large_deletion_flag BOOLEAN,
    cross_domain_flag BOOLEAN,
    processed_at TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (stat_date STRING)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'format-version' = '2'
)
"""



