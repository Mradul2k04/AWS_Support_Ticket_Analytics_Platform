import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns 
import os

# Set page config
st.set_page_config(page_title="AWS Support Ticket Analytics", layout="wide")

st.title("AWS Support Ticket Analytics Dashboard")
st.markdown("Interactive view of live ticket metrics.")

from dotenv import load_dotenv

# Load env variables
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

import sqlalchemy

@st.cache_data
def load_data():
    rds_host = os.getenv("RDS_HOST")
    rds_port = os.getenv("RDS_PORT", "5432")
    rds_db = os.getenv("RDS_DB")
    rds_user = os.getenv("RDS_USER")
    rds_pass = os.getenv("RDS_PASS")
    
    if not all([rds_host, rds_db, rds_user, rds_pass]):
        st.error("RDS connection variables are missing from your .env file.")
        return pd.DataFrame()
        
    # Construct SQLAlchemy Database URL
    db_url = f"postgresql://{rds_user}:{rds_pass}@{rds_host}:{rds_port}/{rds_db}"
    
    try:
        engine = sqlalchemy.create_engine(db_url)
        # Read from fact_tickets table and join dim_projects & dim_agents for readable names
        query = """
            SELECT f.*, p.project_name, a.agent_name 
            FROM fact_tickets f
            LEFT JOIN dim_projects p ON f.project_id = p.project_id
            LEFT JOIN dim_agents a ON f.agent_id = a.agent_id
            LIMIT 500000
        """
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error loading data from RDS: {e}")
        st.info("Make sure you have installed 'SQLAlchemy' and 'psycopg2-binary', and your IP is allowed to access the RDS instance.")
        return pd.DataFrame()

df = load_data()

if not df.empty:
    st.subheader("Data Overview")
    st.dataframe(df.head())

    st.markdown("---")
    st.subheader("Key Performance Indicators")
    kpi1, kpi2, kpi3 = st.columns(3)
    
    # Overall Compliance
    if 'resolution_target_met' in df.columns:
        # Some schemas store this as true/false string, others boolean
        if df['resolution_target_met'].dtype == 'O':
            compliance = (df['resolution_target_met'].astype(str).str.lower() == 'true').mean() * 100
        else:
            compliance = (df['resolution_target_met'] == True).mean() * 100
        kpi1.metric("Overall SLA Compliance", f"{compliance:.1f}%")
        
    # Top Category
    plot_cat_col = 'issue_type' if 'issue_type' in df.columns else ('category' if 'category' in df.columns else None)
    if plot_cat_col:
        top_cat = df[plot_cat_col].mode()[0]
        kpi2.metric("Top Issue Category", top_cat)
        
    # Total Tickets
    kpi3.metric("Total Tickets Processed", f"{len(df):,}")
    
    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Ticket Distribution by Status")
        fig, ax = plt.subplots(figsize=(8, 5))
        plot_col = 'issue_status' if 'issue_status' in df.columns else ('status' if 'status' in df.columns else (df.columns[0] if len(df.columns) > 0 else None))
        
        if plot_col:
            # Safely cap to top 15 to avoid cluttered y-axis
            top_vals = df[plot_col].value_counts().head(15).index
            sns.countplot(data=df[df[plot_col].isin(top_vals)], y=plot_col, ax=ax, palette="viridis", order=top_vals)
            ax.set_title(f"Tickets by {plot_col.replace('_', ' ').title()}")
            st.pyplot(fig)
        else:
            st.write("Not enough columns to plot.")

    with col2:
        st.subheader("Ticket Priorities")
        fig2, ax2 = plt.subplots(figsize=(8, 5))
        plot_col2 = 'issue_priority' if 'issue_priority' in df.columns else ('priority' if 'priority' in df.columns else (df.columns[1] if len(df.columns) > 1 else None))
        
        if plot_col2:
            top_vals2 = df[plot_col2].value_counts().head(10).index
            sns.countplot(data=df[df[plot_col2].isin(top_vals2)], x=plot_col2, ax=ax2, palette="magma", order=top_vals2)
            ax2.set_title(f"Tickets by {plot_col2.replace('_', ' ').title()}")
            plt.xticks(rotation=45)
            st.pyplot(fig2)
        else:
            st.write("Not enough columns to plot.")

    st.markdown("---")
    
    col3, col4 = st.columns(2)
    
    with col3:
        st.subheader("Ticket Types")
        fig3, ax3 = plt.subplots(figsize=(8, 5))
        plot_col3 = 'issue_type' if 'issue_type' in df.columns else ('category' if 'category' in df.columns else (df.columns[2] if len(df.columns) > 2 else None))
        
        if plot_col3:
            top_vals3 = df[plot_col3].value_counts().head(10).index
            sns.countplot(data=df[df[plot_col3].isin(top_vals3)], x=plot_col3, ax=ax3, palette="Set2", order=top_vals3)
            ax3.set_title(f"Tickets by {plot_col3.replace('_', ' ').title()}")
            plt.xticks(rotation=45)
            st.pyplot(fig3)
        else:
            st.write("Not enough columns to plot.")

    with col4:
        st.subheader("Tickets Over Time")
        time_col = 'issue_created' if 'issue_created' in df.columns else ('start_time' if 'start_time' in df.columns else None)
        if time_col:
            # Convert to datetime and plot counts by month/year
            df['created_dt'] = pd.to_datetime(df[time_col], errors='coerce')
            df['year_month'] = df['created_dt'].dt.to_period('M').astype(str)
            time_counts = df['year_month'].value_counts().sort_index()
            
            fig4, ax4 = plt.subplots(figsize=(8, 5))
            # Just plotting top 20 or all if < 20
            if len(time_counts) > 20:
                time_counts = time_counts.tail(20) # show last 20 months
            
            time_counts.plot(kind='bar', ax=ax4, color='skyblue')
            ax4.set_title("Tickets Over Time (Last 20 Months)")
            plt.xticks(rotation=45)
            st.pyplot(fig4)
        else:
            st.info("Time series column not found to plot Tickets Over Time.")

    st.markdown("---")
    
    col5, col6 = st.columns(2)
    
    with col5:
        st.subheader("Avg Resolution Time by Priority")
        if 'resolution_duration_hours' in df.columns and 'priority' in df.columns:
            fig5, ax5 = plt.subplots(figsize=(8, 5))
            # Group by priority and get mean resolution time
            avg_res = df.groupby('priority')['resolution_duration_hours'].mean().sort_values(ascending=False).head(10)
            sns.barplot(x=avg_res.index, y=avg_res.values, ax=ax5, palette="coolwarm")
            ax5.set_title("Average Resolution Hours by Priority")
            ax5.set_ylabel("Hours")
            plt.xticks(rotation=45)
            st.pyplot(fig5)
        else:
            st.info("Missing 'resolution_duration_hours' or 'priority' columns to plot.")

    with col6:
        st.subheader("Resolution Target Compliance")
        if 'resolution_target_met' in df.columns:
            fig6, ax6 = plt.subplots(figsize=(6, 6))
            target_counts = df['resolution_target_met'].value_counts()
            # Green for True/Met, Red for False/Missed
            colors = ['#4CAF50' if str(k).lower() == 'true' else '#F44336' for k in target_counts.index]
            ax6.pie(target_counts, labels=[str(k) for k in target_counts.index], autopct='%1.1f%%', colors=colors, startangle=90)
            ax6.set_title("SLA Target Met")
            st.pyplot(fig6)
        else:
            st.info("Missing 'resolution_target_met' column to plot.")

    st.markdown("---")
    
    st.subheader("Top 15 Projects by Ticket Volume")
    if 'project_name' in df.columns:
        fig7, ax7 = plt.subplots(figsize=(10, 5))
        top_projects = df['project_name'].value_counts().head(15).index
        sns.countplot(data=df[df['project_name'].isin(top_projects)], y='project_name', ax=ax7, palette="cubehelix", order=top_projects)
        ax7.set_title("Top 15 Projects")
        ax7.set_xlabel("Ticket Count")
        ax7.set_ylabel("Project")
        st.pyplot(fig7)
    else:
        st.info("Missing 'project_name' column to plot Top Projects.")

    st.markdown("---")
    st.header("Business Use Cases")

    # Use Case 1
    st.subheader("Use Case 1: Resolution Performance Analysis")
    uc1_col1, uc1_col2 = st.columns(2)
    with uc1_col1:
        if 'resolution_target_met' in df.columns and 'priority' in df.columns:
            # Convert to bool for mean calculation
            if df['resolution_target_met'].dtype == 'O':
                df['target_met_bool'] = (df['resolution_target_met'].astype(str).str.lower() == 'true')
            else:
                df['target_met_bool'] = df['resolution_target_met']
            comp_by_pri = df.groupby('priority')['target_met_bool'].mean().sort_values() * 100
            
            fig_uc1a, ax_uc1a = plt.subplots(figsize=(6,4))
            sns.barplot(x=comp_by_pri.values, y=comp_by_pri.index, ax=ax_uc1a, palette="RdYlGn")
            ax_uc1a.set_title("SLA Compliance % by Priority")
            ax_uc1a.set_xlabel("Compliance (%)")
            st.pyplot(fig_uc1a)
    with uc1_col2:
        if 'target_met_bool' in df.columns and 'year_month' in df.columns:
            # Monthly compliance trend
            monthly_comp = df.groupby('year_month')['target_met_bool'].mean() * 100
            # Limit to last 20 months
            if len(monthly_comp) > 20:
                monthly_comp = monthly_comp.tail(20)
            fig_uc1b, ax_uc1b = plt.subplots(figsize=(6,4))
            monthly_comp.plot(kind='line', marker='o', ax=ax_uc1b, color='green')
            ax_uc1b.set_title("Monthly SLA Compliance Trend")
            ax_uc1b.set_ylabel("Compliance (%)")
            plt.xticks(rotation=45)
            st.pyplot(fig_uc1b)

    # Use Case 2
    st.subheader("Use Case 2: Agent Workload & Performance")
    uc2_col1, uc2_col2 = st.columns(2)
    with uc2_col1:
        if 'agent_name' in df.columns:
            top_agents = df['agent_name'].value_counts().head(10)
            fig_uc2a, ax_uc2a = plt.subplots(figsize=(6,4))
            sns.barplot(x=top_agents.values, y=top_agents.index, ax=ax_uc2a, palette="Blues_r")
            ax_uc2a.set_title("Tickets Resolved per Agent (Top 10)")
            st.pyplot(fig_uc2a)
    with uc2_col2:
        if 'agent_name' in df.columns and 'resolution_duration_hours' in df.columns:
            agent_avg_time = df.groupby('agent_name')['resolution_duration_hours'].mean().loc[top_agents.index]
            fig_uc2b, ax_uc2b = plt.subplots(figsize=(6,4))
            sns.barplot(x=agent_avg_time.values, y=agent_avg_time.index, ax=ax_uc2b, palette="Oranges_r")
            ax_uc2b.set_title("Avg Resolution Time by Agent (Hours)")
            st.pyplot(fig_uc2b)

    # Use Case 3
    st.subheader("Use Case 3: Ticket Category Trends")
    if plot_cat_col and 'year_month' in df.columns:
        fig_uc3, ax_uc3 = plt.subplots(figsize=(12,3))
        # Heatmap of category vs month (Top 10 categories, last 12 months)
        top_cats_10 = df[plot_cat_col].value_counts().head(10).index
        
        # Filter for top 10 categories
        heat_df = df[df[plot_cat_col].isin(top_cats_10)]
        
        # Keep only last 12 months for cleaner heatmap
        recent_months = sorted(df['year_month'].unique())[-12:]
        heat_df = heat_df[heat_df['year_month'].isin(recent_months)]
        
        heat_pivot = heat_df.pivot_table(index=plot_cat_col, columns='year_month', values=df.columns[0], aggfunc='count', fill_value=0)
        sns.heatmap(heat_pivot, cmap="YlGnBu", ax=ax_uc3, linewidths=.5, annot=True, fmt="d")
        ax_uc3.set_title("Ticket Volume Heatmap: Category vs. Month")
        st.pyplot(fig_uc3)

else:
    st.info("No data available to display visualizations.")