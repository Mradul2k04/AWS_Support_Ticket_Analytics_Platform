import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns 
import os, sqlalchemy
from dotenv import load_dotenv

st.set_page_config(page_title="AWS Support Ticket Analytics", layout="wide")
st.title("AWS Support Ticket Analytics Dashboard")
st.markdown("Interactive view of live ticket metrics.")

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

@st.cache_data
def load_data():
    req = ["RDS_HOST", "RDS_PORT", "RDS_DB", "RDS_USER", "RDS_PASS"]
    env = {k: os.getenv(k, "5432" if k == "RDS_PORT" else None) for k in req}
    if not all(env[k] for k in req if k != "RDS_PORT"):
        st.error("RDS connection variables are missing from your .env file.")
        return pd.DataFrame()
    url = f"postgresql://{env['RDS_USER']}:{env['RDS_PASS']}@{env['RDS_HOST']}:{env['RDS_PORT']}/{env['RDS_DB']}"
    try:
        return pd.read_sql("SELECT f.*, p.project_name, a.agent_name FROM fact_tickets f LEFT JOIN dim_projects p ON f.project_id = p.project_id LEFT JOIN dim_agents a ON f.agent_id = a.agent_id LIMIT 500000", sqlalchemy.create_engine(url))
    except Exception as e:
        st.error(f"Error loading data from RDS: {e}")
        st.info("Make sure you have installed 'SQLAlchemy' and 'psycopg2-binary', and your IP is allowed to access the RDS instance.")
        return pd.DataFrame()

df = load_data()

def get_col(*names, default=None):
    return next((n for n in names if n in df.columns), default)

if df.empty:
    st.info("No data available to display visualizations.")
    st.stop()

st.subheader("Data Overview")
st.dataframe(df.head())
st.markdown("---")
st.subheader("Key Performance Indicators")
kpi1, kpi2, kpi3 = st.columns(3)

if 'resolution_target_met' in df.columns:
    df['target_met'] = df['resolution_target_met'].astype(str).str.lower() == 'true'
    kpi1.metric("Overall SLA Compliance", f"{df['target_met'].mean() * 100:.1f}%")
    
c_cat = get_col('issue_type', 'category')
if c_cat: 
    kpi2.metric("Top Issue Category", df[c_cat].mode()[0])
kpi3.metric("Total Tickets Processed", f"{len(df):,}")

st.markdown("---")

def plt_ct(c, pal, title, ax, y=False, top=10):
    if c:
        tops = df[c].value_counts().head(top).index
        sns.countplot(data=df[df[c].isin(tops)], **{'y' if y else 'x': c}, ax=ax, palette=pal, order=tops)
        ax.set_title(title)
        if not y: plt.xticks(rotation=45)
        return True
    return False

c1, c2 = st.columns(2)
with c1:
    st.subheader("Ticket Distribution by Status")
    fig, ax = plt.subplots(figsize=(8, 5))
    c_stat = get_col('issue_status', 'status', default=df.columns[0] if len(df.columns) > 0 else None)
    if plt_ct(c_stat, "viridis", f"Tickets by {str(c_stat).replace('_', ' ').title()}", ax, True, 15): 
        st.pyplot(fig)

with c2:
    st.subheader("Ticket Priorities")
    fig, ax = plt.subplots(figsize=(8, 5))
    c_pri = get_col('issue_priority', 'priority', default=df.columns[1] if len(df.columns) > 1 else None)
    if plt_ct(c_pri, "magma", f"Tickets by {str(c_pri).replace('_', ' ').title()}", ax, False): 
        st.pyplot(fig)

st.markdown("---")
c3, c4 = st.columns(2)

with c3:
    st.subheader("Ticket Types")
    fig, ax = plt.subplots(figsize=(8, 5))
    c_typ = get_col('issue_type', 'category', default=df.columns[2] if len(df.columns) > 2 else None)
    if plt_ct(c_typ, "Set2", f"Tickets by {str(c_typ).replace('_', ' ').title()}", ax, False): 
        st.pyplot(fig)

with c4:
    st.subheader("Tickets Over Time")
    t_col = get_col('issue_created', 'start_time')
    if t_col:
        df['ym'] = pd.to_datetime(df[t_col], errors='coerce').dt.to_period('M').astype(str)
        fig, ax = plt.subplots(figsize=(8, 5))
        df['ym'].value_counts().sort_index().tail(20).plot(kind='bar', ax=ax, color='skyblue')
        ax.set_title("Tickets Over Time (Last 20 Months)")
        plt.xticks(rotation=45)
        st.pyplot(fig)

st.markdown("---")
c5, c6 = st.columns(2)

with c5:
    st.subheader("Avg Resolution Time by Priority")
    if 'resolution_duration_hours' in df.columns and 'priority' in df.columns:
        fig, ax = plt.subplots(figsize=(8, 5))
        s = df.groupby('priority')['resolution_duration_hours'].mean().nlargest(10)
        sns.barplot(x=s.index, y=s.values, ax=ax, palette="coolwarm")
        ax.set(title="Average Resolution Hours by Priority", ylabel="Hours")
        plt.xticks(rotation=45)
        st.pyplot(fig)

with c6:
    st.subheader("Resolution Target Compliance")
    if 'resolution_target_met' in df.columns:
        fig, ax = plt.subplots(figsize=(6, 6))
        tc = df['resolution_target_met'].value_counts()
        ax.pie(tc, labels=tc.index.astype(str), autopct='%1.1f%%', colors=['#4CAF50' if str(k).lower()=='true' else '#F44336' for k in tc.index], startangle=90)
        ax.set_title("SLA Target Met")
        st.pyplot(fig)

st.markdown("---")
st.subheader("Top 15 Projects by Ticket Volume")
if 'project_name' in df.columns:
    fig, ax = plt.subplots(figsize=(10, 5))
    if plt_ct('project_name', "cubehelix", "Top 15 Projects", ax, True, 15):
        ax.set(xlabel="Ticket Count", ylabel="Project")
        st.pyplot(fig)

st.markdown("---")
st.header("Business Use Cases")

st.subheader("Use Case 1: Resolution Performance Analysis")
u1, u2 = st.columns(2)
with u1:
    if 'target_met' in df.columns and 'priority' in df.columns:
        comp_pri = df.groupby('priority')['target_met'].mean().sort_values() * 100
        fig, ax = plt.subplots(figsize=(6,4))
        sns.barplot(x=comp_pri.values, y=comp_pri.index, ax=ax, palette="RdYlGn")
        ax.set(title="SLA Compliance % by Priority", xlabel="Compliance (%)")
        st.pyplot(fig)
with u2:
    if 'target_met' in df.columns and 'ym' in df.columns:
        fig, ax = plt.subplots(figsize=(6,4))
        df.groupby('ym')['target_met'].mean().tail(20).mul(100).plot(kind='line', marker='o', ax=ax, color='green')
        ax.set(title="Monthly SLA Compliance Trend", ylabel="Compliance (%)")
        plt.xticks(rotation=45)
        st.pyplot(fig)

st.subheader("Use Case 2: Agent Workload & Performance")
u3, u4 = st.columns(2)
with u3:
    if 'agent_name' in df.columns:
        top_a = df['agent_name'].value_counts().head(10)
        fig, ax = plt.subplots(figsize=(6,4))
        sns.barplot(x=top_a.values, y=top_a.index, ax=ax, palette="Blues_r")
        ax.set_title("Tickets Resolved per Agent (Top 10)")
        st.pyplot(fig)
with u4:
    if 'agent_name' in df.columns and 'resolution_duration_hours' in df.columns:
        avg_t = df.groupby('agent_name')['resolution_duration_hours'].mean().loc[top_a.index]
        fig, ax = plt.subplots(figsize=(6,4))
        sns.barplot(x=avg_t.values, y=avg_t.index, ax=ax, palette="Oranges_r")
        ax.set_title("Avg Resolution Time by Agent (Hours)")
        st.pyplot(fig)

st.subheader("Use Case 3: Ticket Category Trends")
if c_cat and 'ym' in df.columns:
    fig, ax = plt.subplots(figsize=(12,3))
    heat_df = df[df[c_cat].isin(df[c_cat].value_counts().head(10).index) & df['ym'].isin(sorted(df['ym'].unique())[-12:])]
    sns.heatmap(heat_df.pivot_table(index=c_cat, columns='ym', values=df.columns[0], aggfunc='count', fill_value=0), cmap="YlGnBu", ax=ax, linewidths=.5, annot=True, fmt="d")
    ax.set_title("Ticket Volume Heatmap: Category vs. Month")
    st.pyplot(fig)

