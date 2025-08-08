#Required libraries for the program before install pandas pip install pandas
import pandas as pd #Convert JSON data into a DataFrame
import json #Read JSON files
import os #File and folder operations 
import pymysql  #Connect to MySQL (pymysql.connect(...))


#Step 1: Set path to data for Top Insurance Data set:1

path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\top\insurance\country\india\state"

all_data = {
    'State': [], 'Year': [], 'Quarter': [],
     'Entity_Name': [],
    'Transaction_Count': [], 'Transaction_Amount': []
}

# Loop through all states
states = os.listdir(path)

for state in states:
    state_path = os.path.join(path, state)

    if not os.path.isdir(state_path):
        continue

    years = os.listdir(state_path)

    for year in years:
        year_path = os.path.join(state_path, year)

        if not os.path.isdir(year_path):
            continue

        for file in os.listdir(year_path):
            if file.endswith(".json"):
                quarter = file.replace(".json", "")
                file_path = os.path.join(year_path, file)

                try:
                    with open(file_path, "r") as f:
                        data = json.load(f)

                        for level in ['states', 'districts', 'pincodes']:
                            level_data = data.get('data', {}).get(level, [])
                            
                            if not level_data:
                                continue

                            for item in level_data:
                                all_data['State'].append(state)
                                all_data['Year'].append(year)
                                all_data['Quarter'].append(quarter)                        
                                all_data['Entity_Name'].append(item.get('entityName'))
                                all_data['Transaction_Count'].append(item.get('metric', {}).get('count', 0))
                                all_data['Transaction_Amount'].append(item.get('metric', {}).get('amount', 0))

                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

# Convert to DataFrame
Top_insurance = pd.DataFrame(all_data)
print(Top_insurance.head())
print(Top_insurance.shape)

#Step 2: Set path to data for Top Transactions Data set:2

path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\top\transaction\country\india\state"

# Final data to collect everything
path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\top\insurance\country\india\state"
all_data = {
    'State': [], 'Year': [], 'Quarter': [],
    
    'District_Name': [], 'District_Transactions': [], 'District_Amount': [],
    'Pincode': [], 'Pincode_Transactions': [], 'Pincode_Amount': []
}

# Loop through all state folders
for state in os.listdir(path):
    state_path = os.path.join(path, state)
    if not os.path.isdir(state_path):
        continue

    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        if not os.path.isdir(year_path):
            continue

        for file in os.listdir(year_path):
            if file.endswith(".json"):
                quarter = file.replace(".json", "")
                file_path = os.path.join(year_path, file)

                try:
                    with open(file_path, 'r') as f:
                        raw = json.load(f)
                        data = raw.get('data')

                        if not data:
                            print(f"Skipping: {file_path} (no data)")
                            continue

                        states = data.get("states") or []
                        districts = data.get("districts") or []
                        pincodes = data.get("pincodes") or []

                        max_len = max(len(states), len(districts), len(pincodes))


                        for i in range(max_len):
                            # State-level
                            s_name = states[i]['entityName'] if i < len(states) else None
                            s_txn = states[i]['metric']['count'] if i < len(states) else None
                            s_amt = states[i]['metric']['amount'] if i < len(states) else None

                            # District-level
                            d_name = districts[i]['entityName'] if i < len(districts) else None
                            d_txn = districts[i]['metric']['count'] if i < len(districts) else None
                            d_amt = districts[i]['metric']['amount'] if i < len(districts) else None

                            # Pincode-level
                            p_name = pincodes[i]['entityName'] if i < len(pincodes) else None
                            p_txn = pincodes[i]['metric']['count'] if i < len(pincodes) else None
                            p_amt = pincodes[i]['metric']['amount'] if i < len(pincodes) else None

                            all_data['State'].append(state)
                            all_data['Year'].append(year)
                            all_data['Quarter'].append(quarter)
                            all_data['District_Name'].append(d_name)
                            all_data['District_Transactions'].append(d_txn)
                            all_data['District_Amount'].append(d_amt)
                            all_data['Pincode'].append(p_name)
                            all_data['Pincode_Transactions'].append(p_txn)
                            all_data['Pincode_Amount'].append(p_amt)

                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

# Convert to DataFrame
Top_transactions = pd.DataFrame(all_data)

# Print sample output
print(Top_transactions.head())
print("DataFrame Shape:", Top_transactions.shape)

#Step:3 Set path to data for Top User Data set:3

path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\top\user\country\india\state"

all_data = {
    'State': [], 'Year': [], 'Quarter': [],
    
    'District_Name': [], 'District_RegisteredUsers': [],
    'Pincode': [], 'Pincode_RegisteredUsers': []
}

# Loop through all state folders
for state in os.listdir(path):
    state_path = os.path.join(path, state)
    if not os.path.isdir(state_path):
        continue

    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        if not os.path.isdir(year_path):
            continue

        for file in os.listdir(year_path):
            if file.endswith(".json"):
                quarter = file.replace(".json", "")
                file_path = os.path.join(year_path, file)

                try:
                    with open(file_path, 'r') as f:
                        raw = json.load(f)
                        data = raw.get('data')

                        if not data:
                            print(f"Skipping: {file_path} (no data)")
                            continue

                        states = data.get("states") or []
                        districts = data.get("districts") or []
                        pincodes = data.get("pincodes") or []

                        max_len = max(len(states), len(districts), len(pincodes))

                        for i in range(max_len):
                            # State-level
                            s_name = states[i]['name'] if i < len(states) else None
                            s_users = states[i]['registeredUsers'] if i < len(states) else None

                            # District-level
                            d_name = districts[i]['name'] if i < len(districts) else None
                            d_users = districts[i]['registeredUsers'] if i < len(districts) else None

                            # Pincode-level
                            p_name = pincodes[i]['name'] if i < len(pincodes) else None
                            p_users = pincodes[i]['registeredUsers'] if i < len(pincodes) else None

                            all_data['State'].append(state)
                            all_data['Year'].append(year)
                            all_data['Quarter'].append(quarter)
                            all_data['District_Name'].append(d_name)
                            all_data['District_RegisteredUsers'].append(d_users)
                            all_data['Pincode'].append(p_name)
                            all_data['Pincode_RegisteredUsers'].append(p_users)

                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

# Convert to DataFrame
Top_users = pd.DataFrame(all_data)
print("DataFrame Shape:", Top_users.shape)
Top_users


# Step 1: Connect to MySQL Server
Connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345',
    database='Phonepay_DB'  # DB must already exist
)
print("Connected to MySQL")

# Step 2: Create Cursor
cursor = Connection.cursor()
print("Cursor created")

# Insert Query for Top Insurance Data
insurance_insert_query = """
    INSERT INTO Top_Insurance (
        State, Year, Quarter,
        Entity_Name, Transaction_Count, Transaction_Amount
    )
    VALUES (%s, %s, %s, %s, %s, %s)
"""

insurance_data = [
    (
        row['State'], row['Year'], row['Quarter'],
        row['Entity_Name'], row['Transaction_Count'], row['Transaction_Amount']
    )
    for _, row in Top_insurance.iterrows()
]

cursor.executemany(insurance_insert_query, insurance_data)
print("Top_Insurance data inserted successfully.")

# Insert Query for Top Transaction Data


Top_transactions.fillna({
    'District_Transactions': 0,
    'District_Amount': 0.0,
    'Pincode_Transactions': 0,
    'Pincode_Amount': 0.0,
    'District_Name': '',
    'Pincode': '',
    'State': '',
    'Year': 0,
    'Quarter': 0
}, inplace=True)


transactions_insert_query = """
    INSERT INTO top_transactions (
        State,
        Year,
        Quarter,
        District_Name,
        District_Transactions,
        District_Amount,
        Pincode,
        Pincode_Transactions,
        Pincode_Amount
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
transactions_data = [
    (
        row['State'],int(row['Year']),int(row['Quarter']),row['District_Name'],int(row['District_Transactions']),
        float(row['District_Amount']),row['Pincode'],int(row['Pincode_Transactions']),float(row['Pincode_Amount'])
    )
    for _, row in Top_transactions.iterrows()
]

# Execute insert
cursor.executemany(transactions_insert_query, transactions_data)
print("top_transactions data inserted successfully.")
Connection.commit()

# Fill missing values in the DataFrame
Top_users.fillna({
    'State': '',
    'Year': 0,
    'Quarter': 0,
    'District_Name': '',
    'District_RegisteredUsers': 0.0,
    'Pincode': '',
    'Pincode_RegisteredUsers': 0
}, inplace=True)

# Insert query for top_users table
top_users_insert_query = """
    INSERT INTO top_users (
        State,
        Year,
        Quarter,
        District_Name,
        District_RegisteredUsers,
        Pincode,
        Pincode_RegisteredUsers
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# Prepare the data
top_users_data = [
    (
        row['State'],
        int(row['Year']),
        int(row['Quarter']),
        row['District_Name'],
        float(row['District_RegisteredUsers']),
        row['Pincode'],
        int(row['Pincode_RegisteredUsers'])
    )
    for _, row in Top_users.iterrows()
]

# Execute insert
cursor.executemany(top_users_insert_query, top_users_data)
print("top_users data inserted successfully.")
Connection.commit()

# Step 6: Close connection
cursor.close()
Connection.close()
print("Connection closed")