import streamlit as st
import pandas as pd
import pymysql

# Connect to DB
connection = pymysql.connect(
    host="localhost",
    user="root",
    password="12345",
    database="phonepay_db"
)

st.set_page_config(page_title="PhonePe Insights", layout="wide")
st.title("📊 PhonePe Transaction Casestudy:1")

# Dropdowns
st.sidebar.header("📅 Filter Options")
years = list(range(2018, 2024))
quarters = [1, 2, 3, 4]
selected_year = st.sidebar.selectbox("Select Year", years)
selected_quarter = st.sidebar.selectbox("Select Quarter", quarters)

# Query 1: Top 10 by Transaction Amount
query1 = f"""
SELECT State, Year, Quarter, SUM(Transaction_amount) AS Total_Amount
FROM agg_transaction
WHERE Year = {selected_year} AND Quarter = {selected_quarter}
GROUP BY State, Year, Quarter
ORDER BY Total_Amount DESC
LIMIT 10
"""
df1 = pd.read_sql(query1, connection)
st.subheader("🔝 Top 10 States by Transaction Amount")
st.dataframe(df1)
st.bar_chart(df1.set_index("State")["Total_Amount"])

# Query 2: Yearly Totals
query2 = f"""
SELECT State, Year, 
       SUM(Transaction_count) AS Yearly_Transactions,
       SUM(Transaction_amount) AS Yearly_Amount
FROM agg_transaction
WHERE Year = {selected_year}
GROUP BY State, Year
ORDER BY Yearly_Amount DESC
LIMIT 10
"""
df2 = pd.read_sql(query2, connection)
st.subheader("📅 Yearly Aggregated Transactions")
st.dataframe(df2)
st.line_chart(df2.set_index("State")["Yearly_Amount"])

# Query 3: Transaction Types
query3 = f"""
SELECT Transaction_type, SUM(Transaction_count) AS Total_Transactions
FROM agg_transaction
WHERE Quarter = {selected_quarter} AND Year = {selected_year}
GROUP BY Transaction_type
ORDER BY Total_Transactions ASC
"""
df3 = pd.read_sql(query3, connection)
st.subheader("📂 Transaction Type Trends")
st.dataframe(df3)
st.bar_chart(df3.set_index("Transaction_type")["Total_Transactions"])

# Close connection
connection.close()
