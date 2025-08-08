import streamlit as st
import pymysql
import pandas as pd
import plotly.express as px

# --- Page Setup ---
st.set_page_config(page_title=" Transaction Analysis Across States and DistrictsCasestudy:3", layout="wide")

# --- Sidebar Navigation ---
st.sidebar.title("📌 Navigation")
page = st.sidebar.radio("Select Query View:", [
    "Top 10 States by Transaction Value",
    "Top 10 Districts by Transaction Volume",
    "High-performing States (Quarterly Trend)",
    "Districts with Low Transaction Activity",
    "Year-over-Year Growth by State"
])

# --- MySQL Connection ---
def get_data():
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='12345',
        database='phonepay_db'
    )

    queries = {
        "Top 10 States by Transaction Value": """
            SELECT State, SUM(User_amount) AS Total_Amount, SUM(User_count) AS Total_Users
            FROM map_transaction
            GROUP BY State
            ORDER BY Total_Amount DESC
            LIMIT 10
        """,

        "Top 10 Districts by Transaction Volume": """
            SELECT State, District, SUM(User_count) AS Total_Users, SUM(User_amount) AS Total_Amount
            FROM map_transaction
            GROUP BY State, District
            ORDER BY Total_Users DESC
            LIMIT 10
        """,

        "High-performing States (Quarterly Trend)": """
            SELECT State, Year, Quarter, SUM(User_count) AS Total_Users, SUM(User_amount) AS Total_Amount
            FROM map_transaction
            GROUP BY State, Year, Quarter
            HAVING SUM(User_amount) > 10000000
            ORDER BY Total_Amount DESC
            LIMIT 10
        """,

        "Districts with Low Transaction Activity": """
            SELECT State, District, SUM(User_count) AS Total_Users, SUM(User_amount) AS Total_Amount
            FROM map_transaction
            GROUP BY State, District
            HAVING SUM(User_count) < 1000
            ORDER BY Total_Users ASC
            LIMIT 10
        """,

        "Year-over-Year Growth by State": """
            SELECT State, Year, SUM(User_count) AS Yearly_Users, SUM(User_amount) AS Yearly_Amount
            FROM map_transaction
            GROUP BY State, Year
            ORDER BY State, Year
            LIMIT 10
        """
    }

    df = pd.read_sql(queries[page], connection)
    connection.close()
    return df

# --- Display Data and Visualizations ---
df = get_data()

st.title(f"📈 {page}")
st.dataframe(df)

# --- Chart Visualizations ---
if page == "Top 10 States by Transaction Value":
    fig = px.bar(df, x="State", y="Total_Amount", color="State", title="Top 10 States by Transaction Value")
    st.plotly_chart(fig, use_container_width=True)

elif page == "Top 10 Districts by Transaction Volume":
    fig = px.bar(df, x="District", y="Total_Users", color="State", title="Top 10 Districts by User Count")
    st.plotly_chart(fig, use_container_width=True)

elif page == "High-performing States (Quarterly Trend)":
    fig = px.line(df, x="Quarter", y="Total_Amount", color="State", line_group="Year",
                  title="Quarterly Trend in High-performing States", markers=True)
    st.plotly_chart(fig, use_container_width=True)

elif page == "Districts with Low Transaction Activity":
    fig = px.bar(df, x="District", y="Total_Users", color="State", title="Low Activity Districts (User Count < 1000)")
    st.plotly_chart(fig, use_container_width=True)

elif page == "Year-over-Year Growth by State":
    fig = px.line(df, x="Year", y="Yearly_Amount", color="State", markers=True, title="YoY Growth by State")
    st.plotly_chart(fig, use_container_width=True)