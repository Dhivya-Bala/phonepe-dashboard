import pymysql

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

# Step 3: Function to Create Table
def create_table(cursor, table_name, table_definition):
    query = f"""CREATE TABLE IF NOT EXISTS {table_name} ({table_definition});"""
    print(f"Creating table: {table_name}")
    cursor.execute(query)
    Connection.commit()
    print(f"Table '{table_name}' created successfully!")

# Step 4: Table Definitions
Agg_Transaction_def = """
    State VARCHAR(100),
    Year INT,
    Quarter INT,
    Transaction_type VARCHAR(100),
    Transaction_count INT,
    Transaction_amount DOUBLE
"""

Agg_Insurance_def = """
    State VARCHAR(100),
    Year INT,
    Quarter INT,
    Transaction_type VARCHAR(100),
    Transaction_count INT,
    Transaction_amount DOUBLE
"""

Agg_User_def = """
    State VARCHAR(100),
    Year VARCHAR(100),
    Quarter INT,
    User_count INT,
    Brand_type VARCHAR(100),
    Registered_count INT,
    Percentage_Userdevices FLOAT
"""
Map_Insurance_def = """    State VARCHAR(100),
    Year INT,
    Quarter INT,
    District VARCHAR(100),
    User_count INT,
    User_amount DOUBLE
"""
Map_Transaction_def = """
    State VARCHAR(100),
    Year INT,
    Quarter INT,
    District VARCHAR(100),
    User_count INT,
    User_amount DOUBLE
"""
Map_User_def = """
    State VARCHAR(100),
    Year INT,
    Quarter INT,
    District VARCHAR(100),
    RegisteredUsers INT,
    AppOpens DOUBLE
"""

top_insurance_def = """
    State VARCHAR(100),
    Year INT,
    Quarter INT,
    Entity_Name VARCHAR(100),
    Transaction_Count INT,
    Transaction_Amount FLOAT
"""
top_transactions_def = """

    State VARCHAR(100),
    Year INT,
    Quarter INT,
    District_Name VARCHAR(100),
    District_Transactions INT,
    District_Amount FLOAT,
    Pincode VARCHAR(10),
    Pincode_Transactions INT,
    Pincode_Amount FLOAT
"""
top_users_def = """

    State VARCHAR(100),
    Year INT,
    Quarter INT,
    District_Name VARCHAR(100),
    District_RegisteredUsers FLOAT,
    Pincode VARCHAR(20),
    Pincode_RegisteredUsers INT

"""

# Step 5: Call table creation function with proper table name and definition
create_table(cursor, "Agg_Transaction", Agg_Transaction_def)
create_table(cursor, "Agg_Insurance", Agg_Insurance_def)
create_table(cursor, "Agg_User", Agg_User_def)
create_table(cursor, "Map_Insurance", Map_Insurance_def)
create_table(cursor, "Map_Transaction", Map_Transaction_def)
create_table(cursor, "Map_User", Map_User_def)
create_table(cursor, "top_insurance", top_insurance_def)
create_table(cursor, "top_transactions", top_transactions_def)
create_table(cursor, "top_users", top_users_def)


# Step 6: Close connection
cursor.close()
Connection.close()
print("Connection closed")

