import streamlit as st
import pandas as pd
import pymysql
import plotly.express as px

# --- Streamlit Page Config ---
st.set_page_config(page_title=" Device Dominance & User Engagement", layout="wide")

st.title(" Device Dominance & User Engagement Analysis CaseStudy:5")

# --- MySQL Connection Function ---
@st.cache_data
def run_query(query):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='12345',
        database='phonepay_db'
    )
    df = pd.read_sql(query, connection)
    connection.close()
    return df

# --- Queries ---
query1 = """
SELECT State, Brand_type, Registered_count, Percentage_Userdevices
FROM agg_user
WHERE Registered_count > 1000 
AND Percentage_Userdevices < 0.1
ORDER BY Percentage_Userdevices ASC
LIMIT 10
"""

query2 = """
SELECT Year, Brand_type, SUM(Registered_count) AS Yearly_Registrations
FROM agg_user
GROUP BY Year, Brand_type
ORDER BY Year, Yearly_Registrations DESC
LIMIT 10
"""

query3 = """
SELECT 
Year, 
    Brand_type, 
    SUM(Registered_count) AS Yearly_Registrations
FROM agg_user
GROUP BY Year, Brand_type
ORDER BY Year, Yearly_Registrations DESC
LIMIT 10
"""

# --- Fetch Data ---
df1 = run_query(query1)
df2 = run_query(query2)
df3 = run_query(query3)

# --- Tabs ---
tab1, tab2, tab3 = st.tabs([
    "📉 Underutilized Brands",
    "🏆 Top 5 Device Brands by Year",
    "📈 Brand Trends Over Years"
])

# --- Tab 1 ---
with tab1:
    st.subheader("📉 Underutilized Brands (Low Usage Despite High Registrations)")
    st.dataframe(df1)
    fig1 = px.bar(df1, x="Brand_type", y="Percentage_Userdevices", 
                  color="State", title="Underutilized Brands by Percentage of User Devices")
    st.plotly_chart(fig1, use_container_width=True)

# --- Tab 2 ---
with tab2:
    st.subheader("🏆 Top 5 Device Brands by Registration in Each Year")
    st.dataframe(df2)
    fig2 = px.bar(df2, x="Year", y="Yearly_Registrations", color="Brand_type",
                  title="Top 5 Device Brands by Yearly Registrations", barmode="group")
    st.plotly_chart(fig2, use_container_width=True)

# --- Tab 3 ---
with tab3:
    st.subheader("📈 Brand Trends Over the Years")
    st.dataframe(df3)
    fig3 = px.line(df3, x="Year", y="Yearly_Registrations", color="Brand_type",
                   markers=True, title="Yearly Registration Trends by Brand")
    st.plotly_chart(fig3, use_container_width=True)
