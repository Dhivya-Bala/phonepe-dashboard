import streamlit as st
import pandas as pd
import pymysql
import plotly.express as px
import json
import requests

# --- Streamlit Config ---
st.set_page_config(page_title="📍 PhonePe Insurance Analysis", layout="wide")
st.title("📍 Insurance Transactions in India - PhonePe Analysis Casestudy:2")

# --- Load India States GeoJSON ---
geo_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
india_geojson = requests.get(geo_url).json()

# --- MySQL Connection ---
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345',
    database='phonepay_db'
)

# --- SQL Queries ---

query2 = """
SELECT State, Year, SUM(User_count) AS Yearly_Users, SUM(User_amount) AS Yearly_Amount
FROM map_transaction GROUP BY State, Year ORDER BY State, Year LIMIT 10;
"""
df2 = pd.read_sql(query2, connection)

query3 = """
SELECT State, Year, Quarter, SUM(User_count) AS Quarterly_Users, SUM(User_amount) AS Quarterly_Amount
FROM map_transaction GROUP BY State, Year, Quarter ORDER BY State, Year, Quarter LIMIT 10;
"""
df3 = pd.read_sql(query3, connection)

query4 = """
SELECT State, District, SUM(User_count) AS Total_Users, SUM(User_amount) AS Total_Amount
FROM map_transaction GROUP BY State, District ORDER BY State, Total_Amount DESC LIMIT 10;
"""
df4 = pd.read_sql(query4, connection)

query5 = """
SELECT State, District, SUM(User_count) AS Total_Users, SUM(User_amount) AS Total_Amount
FROM map_transaction GROUP BY State, District
HAVING SUM(User_count) < 10000 ORDER BY Total_Users ASC LIMIT 10;
"""
df5 = pd.read_sql(query5, connection)

connection.close()

# --- SECTION 2: Bar Chart (df2) ---
st.subheader("📊 Year-wise Insurance Uptake (Top 10 entries)")

fig_year = px.bar(
    df2,
    x="State",
    y="Yearly_Amount",
    color="Year",
    text="Yearly_Users",
    barmode="group",
    title="Year-wise Insurance Uptake"
)
st.plotly_chart(fig_year, use_container_width=True)

# --- SECTION 3: Line Chart (df3) ---
st.subheader("📈 Quarterly Insurance Trend (Top 10 entries)")

fig_quarter = px.line(
    df3,
    x="Quarter",
    y="Quarterly_Amount",
    color="State",
    markers=True,
    title="Quarterly Insurance Trend"
)
st.plotly_chart(fig_quarter, use_container_width=True)

# --- SECTION 4: District-wise Uptake (df4) ---
st.subheader("🏙️ Top 10 Districts by Insurance Amount")

fig_dist = px.bar(
    df4,
    x="District",
    y="Total_Amount",
    color="State",
    text="Total_Users",
    title="Top Districts by Total Amount"
)
st.plotly_chart(fig_dist, use_container_width=True)

# --- SECTION 5: Low Activity Districts (df5) ---
st.subheader("⚠️ Districts with Low Insurance Activity (User_count < 10000)")

fig_low = px.bar(
    df5,
    x="District",
    y="Total_Users",
    color="State",
    text="Total_Amount",
    title="Low Activity Districts"
)
st.plotly_chart(fig_low, use_container_width=True)
