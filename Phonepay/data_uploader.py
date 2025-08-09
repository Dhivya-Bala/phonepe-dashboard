#Import os module to interact with the operating system
#import os
# Define the path to the directory containing aggregated state data using my local file system
#path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\aggregated\transaction\country\india\state"
#if os.path.exists(path):
    #Agg_state_list = os.listdir(path) # List all files in the directory
    #print(Agg_state_list)
#else:
   # print("Path does not exist:", path)

#Required libraries for the program before install pandas pip install pandas
import pandas as pd
import json
import os
import pymysql 
# Set path to data for Aggregated Transaction Data set:1

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

# Show the DataFrame
print("DataFrame Shape:", Agg_Trans.shape)
print(Agg_Trans.head())

# Set Path to the Aggregated Insurance data folder Date Set:2

path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\aggregated\insurance\country\india\state"

# Check if the path exists
if not os.path.exists(path):
    print("Path does not exist:", path)
    exit()

# List of states
Ins_state_list = os.listdir(path)

# Initialize dictionary to collect data
Ins = {
    'State': [], 'Year': [], 'Quarter': [],
    'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []
}

# Loop through states, years, quarters, and JSON files
for i in Ins_state_list:
    p_i = os.path.join(path, i)
    if not os.path.isdir(p_i):
        continue

    Ins_yr = os.listdir(p_i)
    for j in Ins_yr:
        p_j = os.path.join(p_i, j)
        if not os.path.isdir(p_j):
            continue

        Ins_yr_list = os.listdir(p_j)
        for k in Ins_yr_list:
            p_k = os.path.join(p_j, k)
            if not os.path.isfile(p_k):
                continue

            with open(p_k, 'r') as Data:
                try:
                    D = json.load(Data)
                    if D.get('data') and 'transactionData' in D['data']:
                        for z in D['data']['transactionData']:
                            name = z['name']
                            count = z['paymentInstruments'][0]['count']
                            amount = z['paymentInstruments'][0]['amount']
                            Ins['Transaction_type'].append(name)
                            Ins['Transaction_count'].append(count)
                            Ins['Transaction_amount'].append(amount)
                            Ins['State'].append(i)
                            Ins['Year'].append(j)
                            Ins['Quarter'].append(int(k.strip('.json')))
                except Exception as e:
                    print(f"Error loading {p_k}: {e}")

# Create DataFrame
Agg_insurance = pd.DataFrame(Ins)

# Output
print("Shape:", Agg_insurance.shape)
print(Agg_insurance.head())

# # Set Path to the Aggregated User data folder Date Set:3

path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\aggregated\user\country\india\state"
user_state_list = os.listdir(path)

#  Define the dictionary OUTSIDE the loop
user = {
    'State': [],
    'Year': [],
    'Quater': [],
    'Brand_type': [],
    'Registered_count': [],
    'Percentage_Userdevices': []
}

# Loop through all JSON files and extract data
for i in user_state_list:
   p_i = os.path.join(path, i)
   user_yr = os.listdir(p_i)

   for j in user_yr:
       p_j = os.path.join(p_i, j)
       user_yr_list = os.listdir(p_j)

       for k in user_yr_list:
            p_k = os.path.join(p_j, k)
            with open(p_k, 'r') as Data:
                D = json.load(Data)

                if D['data']['usersByDevice'] is not None:
                    for z in D['data']['usersByDevice']:
                        Name = z.get('brand', 'Unknown')
                        count = z.get('count', 0)
                        Percentage = z.get('percentage', 0.0)

                        user['Brand_type'].append(Name)
                        user['Registered_count'].append(count)
                        user['Percentage_Userdevices'].append(Percentage)
                        user['State'].append(i)
                        user['Year'].append(j)
                        user['Quater'].append(int(k.strip('.json')))

#  Create DataFrame
Agg_user = pd.DataFrame(user)
print("Shape:", Agg_user.shape)

# Optional: Preview the data
print(Agg_user.head())

# Step 1: Connect to MySQL Server
Connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345',
    database='Phonepay_DB'  # Make sure this DB already exists
)
print(" Connected to MySQL")

# Step 2: Create Cursor
cursor = Connection.cursor()
print("Cursor created")

#Insert data into the MySQL database
# Step 3: Insert Transaction Data
# Ensure the Transaction table exists before inserting data

# ========== Insert Functions ==========
def insert_transaction_data(df):
    # Ensure that 'Transaction_count' does not exceed MySQL INT limits
    # MySQL INT max value is 2147483647
    
    df = df[df['Transaction_count'] <= 2147483647]
    query = """
        INSERT INTO Agg_Transaction (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    data = [tuple(row) for row in df.to_numpy()]
    cursor.executemany(query, data)
    Connection.commit()
    print(" Transaction data inserted")

def insert_insurance_data(df):
    df = df[df['Transaction_count'] <= 2147483647]
    query = """
        INSERT INTO Agg_Insurance (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    data = [tuple(row) for row in df.to_numpy()]
    cursor.executemany(query, data)
    Connection.commit()
    print("Insurance data inserted")

def insert_user_data(df):

    def insert_user_data(df):
     print("Agg_user Columns:", df.columns.tolist())  # Debug
    df = df[['State', 'Year', 'Brand_type', 'Registered_count', 'Percentage_Userdevices']]
    query = """
    INSERT INTO Agg_User (State, Year, Brand_type, Registered_count, Percentage_Userdevices)
    VALUES (%s, %s, %s, %s, %s)
    """
    data = list(df.itertuples(index=False, name=None))
    cursor.executemany(query, data)
    Connection.commit()
    print("User data inserted")
    print("Agg_user Columns:", df.columns.tolist())  # Debug


# ========== Call Insert Functions ==========
insert_transaction_data(Agg_Trans)
insert_insurance_data(Agg_insurance)
insert_user_data(Agg_user)

#  Close connection
Connection.close()
print(" All data inserted and MySQL connection closed.")


#=============================================================================================================
#Map Data
import pandas as pd
import json
import os
import pymysql 

# Set path to data for Map Insurance Data set:1

path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\map\insurance\hover\country\india\state"

Map_Insurance = {
    'State': [], 'Year': [], 'Quarter': [],
    'District': [], 'User_count': [], 'User_amount': []
}

# Loop through each state folder
for state in os.listdir(path):
    state_path = os.path.join(path, state)
    if not os.path.isdir(state_path):
        continue

    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        if not os.path.isdir(year_path):
            continue

        for quarter_file in os.listdir(year_path):
            if not quarter_file.endswith(".json"):
                continue

            quarter = quarter_file.strip(".json")
            file_path = os.path.join(year_path, quarter_file)

            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                # Correct key: hoverDataList (not hoverData)
                hover_data_list = data.get("data", {}).get("hoverDataList", [])

                for entry in hover_data_list:
                    district = entry.get("name", "Unknown")
                    metric = entry.get("metric", [])

                    if metric and isinstance(metric[0], dict):
                        count = metric[0].get("count", 0)
                        amount = metric[0].get("amount", 0.0)
                    else:
                        count = 0
                        amount = 0.0

                    Map_Insurance['State'].append(state)
                    Map_Insurance['Year'].append(year)
                    Map_Insurance['Quarter'].append(int(quarter))
                    Map_Insurance['District'].append(district)
                    Map_Insurance['User_count'].append(count)
                    Map_Insurance['User_amount'].append(amount)

            except Exception as e:
                print(f" Error reading {file_path}: {e}")

#  Create DataFrame
Map_Insurance_df = pd.DataFrame(Map_Insurance)

#  Print sample output
print(" DataFrame Shape:", Map_Insurance_df.shape)
print(Map_Insurance_df.head())

# Optional: Save to CSV
#Map_Insurance_df.to_csv("Map_Insurance_Data.csv", index=False)
Map_Insurance_df

#Map_transaction json to DataFrame sturcture:

#  Set your path to the main 'state' folder
path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\map\transaction\hover\country\india\state"

#  Create a dictionary to store extracted data
Map_Transaction = {
    'State': [], 'Year': [], 'Quarter': [],
    'District': [], 'User_count': [], 'User_amount': []
}

#  Loop through all folders and files
for state in os.listdir(path):
    state_path = os.path.join(path, state)
    if not os.path.isdir(state_path):
        continue

    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        if not os.path.isdir(year_path):
            continue

        for quarter_file in os.listdir(year_path):
            if not quarter_file.endswith(".json"):
                continue

            file_path = os.path.join(year_path, quarter_file)
            quarter = quarter_file.strip(".json")

            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                # Get hoverDataList
                hover_list = data.get("data", {}).get("hoverDataList", [])

                for entry in hover_list:
                    district = entry.get("name", "Unknown")
                    metrics = entry.get("metric", [])

                    if metrics and isinstance(metrics[0], dict):
                        count = metrics[0].get("count", 0)
                        amount = metrics[0].get("amount", 0.0)
                    else:
                        count = 0
                        amount = 0.0

                    # Append to dictionary
                    Map_Transaction['State'].append(state)
                    Map_Transaction['Year'].append(year)
                    Map_Transaction['Quarter'].append(int(quarter))
                    Map_Transaction['District'].append(district)
                    Map_Transaction['User_count'].append(count)
                    Map_Transaction['User_amount'].append(amount)

            except Exception as e:
                print(f" Error reading {file_path}: {e}")

#  Create DataFrame
Map_Transaction_df = pd.DataFrame(Map_Transaction)

#  Print output
print("\n DataFrame Shape:", Map_Transaction_df.shape)
print("\n Map Transaction DataFrame Created Successfully:")
print("\n Sample Records:")
print(Map_Transaction_df.head().to_string(index=False))


#Create Data frame for Map structure
path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\map\user\hover\country\india\state"

#  Create a dictionary to store extracted data
Map_Users = {
    'State': [], 'Year': [], 'Quarter': [],
    'District': [], 'RegisteredUsers': [], 'AppOpens': []
}

#  Traverse all state/year/quarter JSONs
for state in os.listdir(path):
    state_path = os.path.join(path, state)
    if not os.path.isdir(state_path):
        continue

    for year in os.listdir(state_path):
        year_path = os.path.join(state_path, year)
        if not os.path.isdir(year_path):
            continue

        for quarter_file in os.listdir(year_path):
            if not quarter_file.endswith(".json"):
                continue

            quarter = quarter_file.strip(".json")
            file_path = os.path.join(year_path, quarter_file)

            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                #  Access hoverData dictionary
                hover_data = data.get("data", {}).get("hoverData", {})

                for district, values in hover_data.items():
                    registered = values.get("registeredUsers", 0)
                    opens = values.get("appOpens", 0)

                    Map_Users['State'].append(state)
                    Map_Users['Year'].append(year)
                    Map_Users['Quarter'].append(int(quarter))
                    Map_Users['District'].append(district)
                    Map_Users['RegisteredUsers'].append(registered)
                    Map_Users['AppOpens'].append(opens)

            except Exception as e:
                print(f" Error reading {file_path}: {e}")

#  Convert to DataFrame
Map_User_df = pd.DataFrame(Map_Users)

# Print result
print(" DataFrame Shape:", Map_User_df.shape)
print("\n Map Users DataFrame Created Successfully:")
print("\n Sample Data:")
print(Map_User_df.head(10).to_string(index=False))


# Database connection
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345',  # Replace with your password
    database='Phonepay_DB'  # Ensure this database exists
)
cursor = connection.cursor()

# --- Insert into Map_Users ---
insurance_insert_query = """
    INSERT INTO Map_Insurance (State, Year, Quarter, District, User_count, User_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
insurance_data_to_insert = [
    (
        row['State'], row['Year'], row['Quarter'], row['District'],
        row['User_count'], row['User_amount']
    )
    for _, row in Map_Insurance_df.iterrows()
]
cursor.executemany(insurance_insert_query, insurance_data_to_insert)
print(" Map_Insurance data inserted.")

# ---------------------- Insert Data into Map_Transaction ----------------------

transaction_insert_query = """
    INSERT INTO Map_Transaction (State, Year, Quarter, District, User_count, User_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
transaction_data_to_insert = [
    (
        row['State'], row['Year'], row['Quarter'], row['District'],
        row['User_count'], row['User_amount']
    )
    for _, row in Map_Transaction_df.iterrows()
]
cursor.executemany(transaction_insert_query, transaction_data_to_insert)
print("Map_Transaction data inserted.")

# ---------------------- Insert Data into Map_Users ----------------------
# Step 2: Clean data
Map_User_df = Map_User_df.where(pd.notnull(Map_User_df), None)

# Step 3: Prepare insert
user_insert_query = """
    INSERT INTO Map_User(State, Year, Quarter, District, RegisteredUsers, AppOpens)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
user_data_to_insert = [
    (
        row['State'], row['Year'], row['Quarter'], row['District'],
        row['RegisteredUsers'], row['AppOpens']
    )
    for _, row in Map_User_df.iterrows()
]

# Step 4: Insert into DB
cursor.executemany(user_insert_query, user_data_to_insert)
connection.commit()
print(" Map_User data inserted successfully.")

# ---------------------- Commit and Close ----------------------

connection.commit()
cursor.close()
connection.close()
print(" All data inserted successfully into MySQL.")

#=============================================================================================================
#Top Data

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