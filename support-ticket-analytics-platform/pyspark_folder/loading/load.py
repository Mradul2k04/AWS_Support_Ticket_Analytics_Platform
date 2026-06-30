from pyspark.sql import functions as func
from pyspark.sql.types import LongType
from utils.logger import logger
from config.config import BUCKET_NAME,RDS_HOST,RDS_PORT,RDS_DB,RDS_PASS,RDS_USER
import os
from sql.cleanup_tables import cleanup_table

S3_CURATED = F"s3a://{BUCKET_NAME}/curated/"

# RDS Config
RDS_URL = (
    "jdbc:postgresql://"
    "ticketanalytics-db.c980macg6bc6.eu-north-1.rds.amazonaws.com/postgres"
)

JDBC_PROPS = {
    "user":                RDS_USER,
    "password":            RDS_PASS, 
    "driver":              "org.postgresql.Driver",
    "ssl":                 "true",
    "sslmode":             "require",
    "reWriteBatchedInserts": "true",   # performance optimization
    "connectTimeout":        "60",     
    "socketTimeout":         "300", 
}


def write_parquet(df, path , partition_col = None):
    """Write DataFrame to S3 curated zone as Parquet."""
    try:
        if df is None:
            raise ValueError(f"DataFrame is None — cannot write to {path}")

        writer = df.write.mode("overwrite").format("parquet")
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.save(path)
        logger.info(f"Parquet written → {path}")

    except Exception as e:
        logger.error(f"Parquet write FAILED [{path}]: {e}")
        raise RuntimeError(f"Pipeline aborted: Parquet write failed for {path}") from e



def write_rds(df, table_name):
    """
    Write DataFrame to RDS PostgreSQL via JDBC.
    — fails gracefully with clear error.
    — overwrite mode keeps load idempotent.
    """
    try:
        if df is None:
            raise ValueError(f"DataFrame is None — cannot write to {table_name}")

        if not JDBC_PROPS["password"]:
            raise EnvironmentError(
                "RDS_PASS environment variable is not set. "
                "Cannot connect to RDS PostgreSQL."
            )

        df.write.jdbc(
            url=RDS_URL,
            table=table_name,
            mode="append",
            properties=JDBC_PROPS
        )
        logger.info(f"RDS load complete → {table_name}")

    except EnvironmentError as ee:
        logger.error(f"Environment error: {ee}")
        raise

    except Exception as e:
        logger.error(f"RDS load FAILED [{table_name}]: {e}")
        raise RuntimeError(
            f"Pipeline aborted: could not write to RDS table {table_name}."
        ) from e



def build_dim(df, value_col, id_col, name_col):
     """
    Building a dimension table from distinct values in fact_df.
    Assigns surrogate integer keys using monotonically_increasing_id.
    """
     return(
         df.select(func.col(value_col).alias(name_col))
         .filter(func.col(name_col).isNotNull()).distinct()
         .withColumn(id_col, (func.monotonically_increasing_id()+1).cast(LongType()))
     )


def load(fact_df, agent_agg_df, resolution_perf_df, cat_trends_df):
    """
    Full load sequence:
      Step 1 — Build dimension tables from fact_df
      Step 2 — Join surrogate keys back onto fact_df
      Step 3 — Write all tables to S3 as Parquet (partitioned by priority)
      Step 4 — Load all tables into RDS PostgreSQL
    """
    #Step 1 — Building dimension tables from fact_df
    logger.info("Building dimension tables....")
    dim_customers = build_dim(fact_df,"reporter","customer_id","reporter_name")
    dim_agents    = build_dim(fact_df, "assignee",  "agent_id",    "agent_name")
    dim_projects  = build_dim(fact_df, "project",   "project_id",  "project_name")

    logger.info(f"dim_customers rows: {dim_customers.count()}")
    logger.info(f"dim_agents rows:    {dim_agents.count()}")
    logger.info(f"dim_projects rows:  {dim_projects.count()}")

    #Step 2 — Join surrogate keys back onto fact_df
    logger.info("Joining surrogate keys onto fact_df...")

    enriched_fact =(
        fact_df.join(dim_customers,fact_df["reporter"] == dim_customers["reporter_name"],"left")
        .join(dim_agents, fact_df["assignee"] == dim_agents["agent_name"], "left")
        .join(dim_projects, fact_df["project"]  == dim_projects["project_name"], "left")
        .select(
            func.col("ticket_id"),
            func.col("customer_id"),
            func.col("agent_id"),
            func.col("project_id"),
            func.col("category"),
            func.col("priority"),
            func.col("start_time"),
            func.col("resolution_time"),
            func.col("resolution_duration_hours"),
            func.col("status"),
            func.col("resolution_target_met"),
        )
    )

    # Enrich agent_agg with agent_id FK
    agent_perf_df = (
        agent_agg_df
        .join(dim_agents,
              agent_agg_df["assignee"] == dim_agents["agent_name"], "left")
        .select(
            func.col("agent_id"),
            func.col("tickets_resolved"),
            func.col("avg_resolution_duration_hours"),
            func.col("rank_by_volume"),
        )
    )

    #Step 3 — Write all tables to S3 as Parquet (partitioned by month)
    logger.info("Writing Parquet to S3 curated zone...")

    enriched_fact.cache()
    enriched_fact.count() 
    logger.info("enriched_fact cached...")
   

    write_parquet(enriched_fact,   f"{S3_CURATED}fact_tickets/",partition_col="priority")
    logger.info("fact_tickets Parquet write complete...")
    #cache release
    enriched_fact.unpersist()
    write_parquet(dim_customers,f"{S3_CURATED}dim_customers/")
    write_parquet(dim_agents,f"{S3_CURATED}dim_agents/")
    write_parquet(dim_projects,f"{S3_CURATED}dim_projects/")
    write_parquet(agent_perf_df,f"{S3_CURATED}agent_performance/")
    write_parquet(resolution_perf_df, f"{S3_CURATED}resolution_performance/")
    write_parquet(cat_trends_df,f"{S3_CURATED}category_trends/")

    logger.info("All Parquet writes complete...")

    # Step 4 — RDS PostgreSQL loads
    try:
        logger.info("Step 4 — Loading tables into RDS PostgreSQL...")
        cleanup_table()
        # Dimensions first (FK dependencies)
        write_rds(dim_customers,"dim_customers")
        write_rds(dim_agents,"dim_agents")
        write_rds(dim_projects,"dim_projects")

        # Fact table
        write_rds(enriched_fact,"fact_tickets")

        # Analytics tables
        write_rds(agent_perf_df,"analytics.agent_performance")
        write_rds(resolution_perf_df, "analytics.resolution_performance")
        write_rds(cat_trends_df,"analytics.category_trends")

        logger.info("Step 4 complete")

    except Exception as e:
        logger.error(f"Step 4 FAILED — RDS load error: {e}")
        raise

    logger.info("Load phase finished — pipeline complete ")