import psycopg2
from config.config import RDS_USER,RDS_DB,RDS_HOST,RDS_PASS


def cleanup_table():
    conn = psycopg2.connect(
        host=RDS_HOST,
        database=RDS_DB,
        user=RDS_USER,
        password=RDS_PASS
    )

    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE fact_tickets CASCADE;")
    cur.execute("TRUNCATE TABLE dim_customers CASCADE;")
    cur.execute("TRUNCATE TABLE dim_agents CASCADE;")
    cur.execute("TRUNCATE TABLE dim_projects CASCADE;")
    cur.execute("TRUNCATE TABLE analytics.agent_performance")
    cur.execute("TRUNCATE TABLE analytics.resolution_performance")
    cur.execute("TRUNCATE TABLE analytics.category_trends")

    conn.commit()
    conn.close()