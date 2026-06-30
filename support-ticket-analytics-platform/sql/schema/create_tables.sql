
-- Analytics schema
CREATE SCHEMA IF NOT EXISTS analytics;


-- Dimension Tables
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id   BIGINT       PRIMARY KEY,
    reporter_name VARCHAR(120) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_agents (
    agent_id   BIGINT       PRIMARY KEY,
    agent_name VARCHAR(120) UNIQUE NOT NULL
);


CREATE TABLE IF NOT EXISTS dim_projects (
    project_id   BIGINT       PRIMARY KEY,
    project_name VARCHAR(120) UNIQUE NOT NULL
);


-- Fact Table
CREATE TABLE IF NOT EXISTS fact_tickets (
    ticket_id                  BIGINT        PRIMARY KEY,
    customer_id                BIGINT        REFERENCES dim_customers(customer_id),
    agent_id                   BIGINT        REFERENCES dim_agents(agent_id),
    project_id                 BIGINT        REFERENCES dim_projects(project_id),
    category                   VARCHAR(60),
    priority                   VARCHAR(20),
    start_time                 TIMESTAMP,
    resolution_time            TIMESTAMP,
    resolution_duration_hours  DECIMAL(10,2),
    status                     VARCHAR(60),
    resolution_target_met      BOOLEAN
);

-- Analytics Tables
CREATE TABLE IF NOT EXISTS analytics.agent_performance (
    agent_id                      BIGINT,
    tickets_resolved              INT,
    avg_resolution_duration_hours DECIMAL(10,2),
    rank_by_volume                INT
);

CREATE TABLE IF NOT EXISTS analytics.resolution_performance (
    priority         VARCHAR(20),
    month            TIMESTAMP,
    tickets_resolved INT,
    compliance_rate  DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS analytics.category_trends (
    category          VARCHAR(60),
    month             TIMESTAMP,
    ticket_volume     INT,
    volume_change_pct DECIMAL(6,2)
);


