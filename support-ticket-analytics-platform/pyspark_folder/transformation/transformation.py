from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as func
from utils.logger import logger

# SLA thresholds per priority
PRIORITY_THRESHOLDS = {
    'Critical':4,
    'High':24,
    'Medium':72,
    'Low':168
}

# Valid priorities
VALID_PRIORITIES = set(PRIORITY_THRESHOLDS.keys())

# Valid statuses found in FEATURES.md
VALID_STATUSES = {
    "Open", "In Progress", "Resolved", "Waiting", "Validation",
    "Resolved Under Monitoring", "Pending Deployment", "Closed",
    "Reopened", "Cancelled", "In Review", "To Do",
    "Under Review", "Rejected", "Done", "Approved"
}

def parse_timestamp(col_name):
    """
    Handles all 3 timestamp formats found in the dataset:
    1. '2016-03-24 15:35:53+00:00'     → strip +00:00
    2. '2018-08-14 15:42:12.564000'    → strip microseconds
    3. '2018-08-14 15:42:12'          
    """
    return func.to_timestamp(
        func.regexp_replace(
            func.regexp_replace(
                func.col(col_name),
                r"\+00:00$", ""           # remove timezone offset
            ),
            r"\.(\d+)$", ""              # remove microseconds/milliseconds
        ),
        "yyyy-MM-dd HH:mm:ss"
    )

#TASK 2
def derive_current_state(change_df):
    #pyspark Task 2 - Derive the most recent assignee and status per ticket from issues_change_history.csv using a window function.
    latest_df = change_df.withColumn(
        "rn", func.row_number().over(
            Window.partitionBy("issueid","field").orderBy(func.col("created").desc())
        )
    ).filter(func.col("rn") == 1)

    # Latest assignee per ticket  (field = "assignee")
    latest_assignee_df = latest_df.filter(func.col("field") == "assignee")\
                        .select(
                            func.col("issueid"),
                            func.col("value").alias("current_assignee"),
                            func.col("created").alias("last_assigned_at")
                        )
    
    # Latest status per ticket  (field = "status")
    latest_status_df = latest_df.filter(func.col("field") == "status")\
                        .select(
                            func.col("issueid"),
                            func.col("value").alias("current_status")
                        )
    
    return (latest_assignee_df,latest_status_df)

def transform(issues_df , change_df):
    #Fix timezone-aware timestamps (+00:00 format) 
    # The CSV contains '2016-03-24 15:35:53+00:00' which Spark 3.x 
    # cannot parse directly as TimestampType.
    # We read them as strings and convert using to_timestamp after 
    # stripping the timezone offset.

    for col_name in ["issue_created", "issue_resolution_date", "last_change_date"]:
        issues_df = issues_df.withColumn(col_name, parse_timestamp(col_name))

    change_df = change_df.withColumn("created", parse_timestamp("created"))

    
    #PySpark transformation tasks.

    #TASK 3 - Filter out tickets with a resolution_time earlier than start_time.
    invalid_counts = issues_df.filter(
        func.col("issue_resolution_date").isNotNull() &
        (func.col("issue_resolution_date") < func.col("issue_created"))
    ).count()
    logger.info(f"Rows flagged for invalid resolution time: {invalid_counts}")

    issues_clean_df = issues_df.filter(
        func.col("issue_resolution_date").isNull() |
        (func.col("issue_resolution_date") >=  func.col("issue_created"))
    )

    #TASK 2 — Derive current state from change history
    latest_assignee_df, latest_status_df = derive_current_state(change_df)

    #TASK 4 - Join issues.csv with the derived current-state view to build a denormalized working DataFrame.
    working_df = (issues_clean_df.join(latest_assignee_df, issues_clean_df["id"] == latest_assignee_df["issueid"], how="left").drop("issueid")\
                .join(latest_status_df, issues_clean_df["id"] == latest_status_df["issueid"], how="left").drop("issueid")
    )

    #TASK 5 - Compute resolution_duration_hours as a derived column.
    working_df = working_df.withColumn(
        "resolution_duration_hours",func.when(func.col("issue_resolution_date").isNotNull(), \
        (func.unix_timestamp("issue_resolution_date") -
        func.unix_timestamp("issue_created")) / 3600.0)\
        .otherwise(func.lit(None))
    )

    #TASK 6 - Deduplicate tickets using a window function partitioned by ticket_id.
    working_df = working_df.withColumn("deduplicate_ticket", func.row_number().over(Window.partitionBy("id").orderBy(func.col("issue_created").asc()))).filter(func.col("deduplicate_ticket") == 1).drop("deduplicate_ticket")
    logger.info(f"Rows after deduplication: {working_df.count()}")

    #TASK 9 - Standardize category and project text fields 
    # issue_type (category in project terminology)
    working_df = working_df.withColumn(
        "issue_type",
        func.when(func.col("issue_type").isNull(), "Uncategorized")
         .otherwise(func.initcap(func.trim(func.col("issue_type"))))
    )

    # issue_priority — title-case and flag unknowns
    working_df = working_df.withColumn(
        "issue_priority",
        func.initcap(func.trim(func.col("issue_priority")))
    )
    valid_prio_list = list(VALID_PRIORITIES)
    working_df = working_df.withColumn(
        "priority_flag",
        func.when(
            func.col("issue_priority").isNotNull() &
            ~func.col("issue_priority").isin(valid_prio_list),
            "UNRECOGNIZED_PRIORITY"
        ).otherwise(func.lit(None))
    )

    # issue_proj — uppercase, trim
    working_df = working_df.withColumn(
        "issue_proj",
        func.upper(func.trim(func.col("issue_proj")))
    )

    #Compute resolution_target_met
    threshold_expr = func.lit(None).cast("double")
    for priority,hours in PRIORITY_THRESHOLDS.items():
        threshold_expr = func.when(func.col("issue_priority") == priority , func.lit(float(hours))).otherwise(threshold_expr)

    working_df = working_df.withColumn(
        "resolution_target_met", func.when(
            func.col("resolution_duration_hours").isNotNull() &
            threshold_expr.isNotNull(),func.col("resolution_duration_hours") <= threshold_expr
        ).otherwise(func.lit(None).cast("boolean"))
    )

    #Build fact_df with final column names matching DB schema 
    fact_df = working_df.select(
        func.col("id").alias("ticket_id"),
        func.col("issue_reporter").alias("reporter"),
        func.coalesce(
            func.col("current_assignee"),
            func.col("issue_assignee")
        ).alias("assignee"),                            # fallback to issues.csv value
        func.col("issue_proj").alias("project"),
        func.col("issue_type").alias("category"),          # issue_type = category domain
        func.col("issue_priority").alias("priority"),
        func.col("issue_created").alias("start_time"),
        func.col("issue_resolution_date").alias("resolution_time"),
        func.col("resolution_duration_hours"),
        func.col("current_status").alias("status"),
        func.col("resolution_target_met"),
        func.col("priority_flag"),
        func.col("issue_contr_count"),
        func.col("processing_steps"),
        func.col("wf_total_time"),
    
    )

    #Task 7 — Aggregate ticket count & avg resolution per agent
    agent_agg_df = fact_df.filter(func.col("resolution_time").isNotNull()).groupBy("assignee").agg(
        func.count("ticket_id").alias("tickets_resolved"),
        func.round(func.avg("resolution_duration_hours"),2).alias("avg_resolution_duration_hours")
    )

    #Task 8 — Rank agents by tickets resolved (window function)
    agent_agg_df = agent_agg_df.withColumn("rank_by_volume", func.dense_rank().over(Window.orderBy(func.col("tickets_resolved").desc())))


    #Use Case 1: Resolution Performance Analysis
    resolution_perf_df  = fact_df.filter(func.col("resolution_time").isNotNull())\
                         .withColumn("month", func.date_trunc("month", func.col("resolution_time")))\
                         .groupBy("priority","month")\
                         .agg(func.count("ticket_id").alias("tickets_resolved"),
                              func.round(100.0 * func.sum(func.col("resolution_target_met").cast("int"))/ func.count("ticket_id"), 2)\
                         .alias("compliance_rate"))

    # Use Case 3: Ticket Category Trends
    cat_monthly = fact_df.withColumn("month", func.date_trunc("month", func.col("start_time")))\
                .groupBy("category", "month")\
                .agg(func.count("ticket_id").alias("ticket_volume"))
    
    # Calculate MoM change
    mom_window= Window.partitionBy("category").orderBy("month")
    cat_trends_df = (
        cat_monthly
        .withColumn("prev_volume", func.lag("ticket_volume").over(mom_window))
        .withColumn(
            "volume_change_pct",
            func.when(
                func.col("prev_volume").isNotNull() & (func.col("prev_volume") > 0),
                func.round(
                    100.0 * (func.col("ticket_volume") - func.col("prev_volume"))
                    / func.col("prev_volume"), 2
                )
            ).otherwise(func.lit(None))
        )
        .drop("prev_volume")
    )
    logger.info(f"Fact table count: {fact_df.count()}")
    logger.info(resolution_perf_df.count())
    

    logger.info("All transformation tasks complete.")
    return fact_df, agent_agg_df, resolution_perf_df, cat_trends_df
