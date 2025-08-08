import os
import json
import pandas as pd
import streamlit as st
import plotly.express as px
import requests

# --- Streamlit Page Config ---
st.set_page_config(page_title="📍 PhonePe Transactions - India Map", layout="wide")
st.title("📍 PhonePe Transactions - India Map")

# ==========================
# 1️⃣ Load and Process Data
# ==========================
path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\aggregated\transaction\country\india\state"

Agg_state_list = os.listdir(path)

Trans = {
    'State': [], 'Year': [], 'Quarter': [],
    'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []
}

for i in Agg_state_list:
    p_i = os.path.join(path, i)
    if not os.path.isdir(p_i):
        continue

    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = os.path.join(p_i, j)
        if not os.path.isdir(p_j):
            continue

        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = os.path.join(p_j, k)
            if not os.path.isfile(p_k):
                continue

            with open(p_k, 'r') as Data:
                try:
                    D = json.load(Data)
                    if D.get('data') and isinstance(D['data'], dict) and 'transactionData' in D['data']:
                        for z in D['data']['transactionData']:
                            name = z['name']
                            count = z['paymentInstruments'][0]['count']
                            amount = z['paymentInstruments'][0]['amount']
                            Trans['Transaction_type'].append(name)
                            Trans['Transaction_count'].append(count)
                            Trans['Transaction_amount'].append(amount)
                            Trans['State'].append(i)
                            Trans['Year'].append(j)
                            Trans['Quarter'].append(int(k.strip('.json')))
                except Exception as e:
                    print(f"Error loading {p_k}: {e}")

Agg_Trans = pd.DataFrame(Trans)

# ==========================
# 2️⃣ State Name Formatting
# ==========================
# Match state names in GeoJSON
state_mapping = {
    'andaman-&-nicobar-islands': 'Andaman & Nicobar Island',
    'andhra-pradesh': 'Andhra Pradesh',
    'arunachal-pradesh': 'Arunanchal Pradesh',
    'assam': 'Assam',
    'bihar': 'Bihar',
    'chandigarh': 'Chandigarh',
    'chhattisgarh': 'Chhattisgarh',
    'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra & Nagar Haveli & Daman & Diu',
    'delhi': 'NCT of Delhi',
    'goa': 'Goa',
    'gujarat': 'Gujarat',
    'haryana': 'Haryana',
    'himachal-pradesh': 'Himachal Pradesh',
    'jammu-&-kashmir': 'Jammu & Kashmir',
    'jharkhand': 'Jharkhand',
    'karnataka': 'Karnataka',
    'kerala': 'Kerala',
    'ladakh': 'Ladakh',
    'lakshadweep': 'Lakshadweep',
    'madhya-pradesh': 'Madhya Pradesh',
    'maharashtra': 'Maharashtra',
    'manipur': 'Manipur',
    'meghalaya': 'Meghalaya',
    'mizoram': 'Mizoram',
    'nagaland': 'Nagaland',
    'odisha': 'Odisha',
    'puducherry': 'Puducherry',
    'punjab': 'Punjab',
    'rajasthan': 'Rajasthan',
    'sikkim': 'Sikkim',
    'tamil-nadu': 'Tamil Nadu',
    'telangana': 'Telangana',
    'tripura': 'Tripura',
    'uttar-pradesh': 'Uttar Pradesh',
    'uttarakhand': 'Uttarakhand',
    'west-bengal': 'West Bengal'
}

Agg_Trans['State'] = Agg_Trans['State'].map(state_mapping)

# ==========================
# 3️⃣ Aggregating Data for Map
# ==========================
# Example: Sum transaction amount for 2020 Q1
year = st.selectbox("Select Year", sorted(Agg_Trans["Year"].unique()))
quarter = st.selectbox("Select Quarter", sorted(Agg_Trans["Quarter"].unique()))

map_df = Agg_Trans[(Agg_Trans["Year"] == year) & (Agg_Trans["Quarter"] == quarter)]
map_df = map_df.groupby("State", as_index=False)["Transaction_amount"].sum()

# ==========================
# 4️⃣ Load GeoJSON
# ==========================
geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
geojson_data = requests.get(geojson_url).json()

# ==========================
# 5️⃣ Plot Choropleth Map
# ==========================
fig = px.choropleth(
    map_df,
    geojson=geojson_data,
    featureidkey='properties.ST_NM',
    locations='State',
    color='Transaction_amount',
    color_continuous_scale='Viridis',
    title=f"Total Transaction Amount - {year} Q{quarter}"
)

fig.update_geos(fitbounds="locations", visible=False)
st.plotly_chart(fig, use_container_width=True)
