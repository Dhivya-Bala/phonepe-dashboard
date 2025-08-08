import streamlit as st
import pandas as pd
import plotly.express as px
import pymysql
import json
import requests

# --- Streamlit Page Config ---
st.set_page_config(page_title="📍 PhonePe India Map", layout="wide")
st.title("📍 PhonePe Top Transactions - India Map")

# Load GeoJSON for India states
geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
india_states_geojson = requests.get(geojson_url).json()

# --- Connect to MySQL ---
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345',
    database='Phonepay_DB'
)

query = """
SELECT State, SUM(District_Amount) AS Total_Amount
FROM top_transactions
GROUP BY State
"""
df = pd.read_sql(query, connection)
connection.close()

# Optional: State name formatting if mismatches occur
df['State'] = df['State'].str.title()

# Plotly Choropleth
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
