import streamlit as st
import pandas as pd
import pymysql
import plotly.express as px

# --- Streamlit Page Config ---
st.set_page_config(page_title="📊 Insurance Transactions Analysis", layout="wide")
st.title("📊 Map Insurance Transactions Analysis Dashboard CaseStudy:4")

# --- Sidebar Filters ---
st.sidebar.header("Filter Data")
year = st.sidebar.selectbox("Select Year", [2018, 2019, 2020, 2021, 2022, 2023])
quarter = st.sidebar.selectbox("Select Quarter", [1, 2, 3, 4])

# --- MySQL Connection ---
@st.cache_resource
def get_connection():
    """Create and cache a MySQL connection."""
    return pymysql.connect(
        host='localhost',
        user='root',
        password='12345',
        database='phonepay_db'
    )

# --- Query Function ---
def run_query(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    return df

# --- Queries with Filters ---
df1 = run_query("""
    SELECT State, SUM(User_amount) AS Total_Amount, SUM(User_count) AS Total_Users
    FROM map_insurance
    GROUP BY State
    ORDER BY Total_Amount DESC
    LIMIT 10;
""")

df2 = run_query(f"""
    SELECT State, District, SUM(User_count) AS Total_Users, SUM(User_amount) AS Total_Amount
    FROM map_insurance
    WHERE Year = {year} AND Quarter = {quarter}
    GROUP BY State, District
    ORDER BY Total_Users DESC
    LIMIT 10;
""")

df3 = run_query("""
    SELECT State, Year, Quarter, SUM(User_amount) AS Total_Amount, SUM(User_count) AS Total_Users
    FROM map_insurance
    GROUP BY State, Year, Quarter
    ORDER BY State, Year, Quarter
    LIMIT 10;
""")

df4 = run_query(f"""
    SELECT State, District, SUM(User_count) AS Total_Users, SUM(User_amount) AS Total_Amount
    FROM map_insurance
    WHERE Year = {year} AND Quarter = {quarter}
    GROUP BY State, District
    HAVING SUM(User_count) < 1000
    ORDER BY Total_Users ASC
    LIMIT 10;
""")

df5 = run_query("""
    SELECT State, Year, SUM(User_count) AS Yearly_Users, SUM(User_amount) AS Yearly_Amount
    FROM map_insurance
    GROUP BY State, Year
    ORDER BY State, Year
    LIMIT 10;
""")

# --- Tabs Layout ---
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏆 Top States by Amount",
    "🏙 Top Districts by Volume",
    "📈 Quarterly Trends",
    "⚠ Low Activity Districts",
    "📊 YoY Growth"
])

# --- Tab 1: Top States ---
with tab1:
    st.subheader(f"Top 10 States by Insurance Transaction Amount ({year} Q{quarter})")
    fig = px.bar(df1, x='State', y='Total_Amount', color='Total_Amount',
                 title="Top States by Insurance Amount", height=500)
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df1)

# --- Tab 2: Top Districts ---
with tab2:
    st.subheader(f"Top 10 Districts by Insurance Transaction Volume ({year} Q{quarter})")
    fig = px.bar(df2, x='District', y='Total_Users', color='Total_Users',
                 title="Top Districts by User Count", height=500)
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df2)

# --- Tab 3: Quarterly Trends ---
with tab3:
    st.subheader("Insurance Quarterly Trends by State")
    fig = px.line(df3, x='Quarter', y='Total_Amount', color='State',
                  markers=True, title="Quarterly Amount Trend")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df3)

# --- Tab 4: Low Activity Districts ---
with tab4:
    st.subheader(f"Districts with Low Insurance Activity (<1000 Users) ({year} Q{quarter})")
    fig = px.bar(df4, x='District', y='Total_Users', color='Total_Users',
                 title="Low Activity Districts", height=500)
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df4)

# --- Tab 5: Year-over-Year Growth ---
with tab5:
    st.subheader("Year-over-Year Insurance Growth by State")
    fig = px.line(df5, x='Year', y='Yearly_Amount', color='State',
                  markers=True, title="Yearly Insurance Amount Growth")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df5)

