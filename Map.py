import os
# Define the path to the directory containing aggregated state data using my local file system
path = r"C:\Users\DhivyaBharthi\Desktop\GUVI\Phonepay\data\map\insurance\country\india\state"
#if os.path.exists(path):
#    Agg_state_list = os.listdir(path) # List all files in the directory
 #   print(Agg_state_list)
#else:
 #  print("Path does not exist:", path)

   #Required libraries for the program before install pandas pip install pandas

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