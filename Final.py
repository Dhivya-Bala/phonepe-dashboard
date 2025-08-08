import streamlit as st
import pandas as pd
import pymysql
import requests
import plotly.express as px


# ---------- DATABASE CONNECTION ----------
def get_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="12345",
        database="Phonepay_DB"
    )


# ---------- CONFIG ----------
st.set_page_config(page_title="📱 PhonePe Dashboard", layout="wide")
st.title("📱 PhonePe Dashboard")

# ---------- SIDEBAR NAVIGATION ----------
st.sidebar.title("📑 Navigation")
page = st.sidebar.radio(
    "Choose a Case Study and PhonePe India Map",
    [
        "Phonepe Map: India Top_Transactions",
        "Case Study 1: Aggregated Transactions",
        "Case Study 2: Insurance Transactions",
        "Case Study 3: State & District Analysis",
        "Case Study 4: Insurance Analysis",
        "Case Study 5: Device Usage & Engagement"
    ]
)

# ---------- PAGE 1: PHONEPE MAP ----------
if page == "Phonepe Map: India Top_Transactions":
    st.title("📍 PhonePe India Transaction Map")

    # Load GeoJSON
    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    india_states_geojson = requests.get(geojson_url).json()

    # Query
    connection = get_connection()
    query = """
        SELECT State, SUM(District_Amount) AS Total_Amount
        FROM top_transactions
        GROUP BY State
    """
    df = pd.read_sql(query, connection)
    connection.close()

    df['State'] = df['State'].str.title()

    # Plot
    fig = px.choropleth(
        df,
        geojson=india_states_geojson,
        featureidkey='properties.ST_NM',
        locations='State',
        color='Total_Amount',
        color_continuous_scale='ice',
        title="📊 State-wise Transaction Amount"
    )
    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig, use_container_width=True)

# ---------- PAGE 2: CASE STUDY 1 ----------
elif page == "Case Study 1: Aggregated Transactions":
    st.title("📊 PhonePe Transaction Case Study 1")

    # Filters
    st.sidebar.header("📅 Filter Options")
    years = list(range(2018, 2024))
    quarters = [1, 2, 3, 4]
    selected_year = st.sidebar.selectbox("Select Year", years)
    selected_quarter = st.sidebar.selectbox("Select Quarter", quarters)

    # Query
    connection = get_connection()
    query1 = f"""
        SELECT State, Year, Quarter, SUM(Transaction_amount) AS Total_Amount
        FROM agg_transaction
        WHERE Year = {selected_year} AND Quarter = {selected_quarter}
        GROUP BY State, Year, Quarter
        ORDER BY Total_Amount DESC
        LIMIT 10
    """
    df1 = pd.read_sql(query1, connection)
    connection.close()

    st.subheader("🔝 Top 10 States by Transaction Amount")
    st.dataframe(df1)
    st.bar_chart(df1.set_index("State")["Total_Amount"])

# ---------- PAGE 3: CASE STUDY 2 ----------
elif page == "Case Study 2: Insurance Transactions":
    st.title("💼 Case Study 2: Insurance Transactions")

    # Filters
    st.sidebar.header("📅 Filter Options")
    years = list(range(2018, 2024))
    quarters = [1, 2, 3, 4]
    selected_year = st.sidebar.selectbox("Select Year", years)
    selected_quarter = st.sidebar.selectbox("Select Quarter", quarters)

    # Query
    connection = get_connection()
    query2 = """
        SELECT State, SUM(User_count) AS Total_Users, SUM(User_amount) AS Total_Amount
        FROM map_transaction
        GROUP BY State
        ORDER BY Total_Amount DESC
        LIMIT 10
    """
    df2 = pd.read_sql(query2, connection)
    connection.close()

    fig_year = px.bar(
        df2,
        x="State",
        y="Total_Amount",
        text="Total_Users",
        title="Year-wise Insurance Uptake (Top 10 States)"
    )
    st.plotly_chart(fig_year, use_container_width=True)

# ---------- PAGE 4: CASE STUDY 3 ----------
elif page == "Case Study 3: State & District Analysis":
    st.title("🌍 Case Study 3: State & District Analysis")

    # Query
    connection = get_connection()
    query3 = """
        SELECT State, District, SUM(User_count) AS Total_Users, SUM(User_amount) AS Total_Amount
        FROM map_transaction
        GROUP BY State, District
        ORDER BY Total_Users DESC
        LIMIT 10
    """
    df3 = pd.read_sql(query3, connection)
    connection.close()

    fig = px.bar(df3, x="District", y="Total_Users", color="State", title="Top 10 Districts by User Count")
    st.plotly_chart(fig, use_container_width=True)

# ---------- PAGE 5: CASE STUDY 4 ----------
elif page == "Case Study 4: Insurance Analysis":
    st.title("📉 Case Study 4: Insurance Analysis")

    connection = get_connection()
    query4 = """
        SELECT State, SUM(User_amount) AS Total_Amount, SUM(User_count) AS Total_Users
        FROM map_insurance
        GROUP BY State
        ORDER BY Total_Amount DESC
        LIMIT 10
    """
    df4 = pd.read_sql(query4, connection)
    connection.close()

    fig = px.bar(df4, x='State', y='Total_Amount', color='Total_Amount',
                 title="Top States by Insurance Amount", height=500)
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df4)

# ---------- PAGE 6: CASE STUDY 5 ----------
elif page == "Case Study 5: Device Usage & Engagement":
    st.title("📱 Case Study 5: Device Usage & Engagement")

    connection = get_connection()
    query5 = """
        SELECT State, Brand_type, Registered_count, Percentage_Userdevices
        FROM agg_user
        WHERE Registered_count > 1000 
        AND Percentage_Userdevices < 0.1
        ORDER BY Percentage_Userdevices ASC
        LIMIT 10
    """
    df5 = pd.read_sql(query5, connection)
    connection.close()

    fig1 = px.bar(df5, x="Brand_type", y="Percentage_Userdevices", 
                  color="State", title="Underutilized Brands by % of User Devices")
    st.plotly_chart(fig1, use_container_width=True)
    st.dataframe(df5)
