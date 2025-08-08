import pymysql


# Step 1: Connect to MySQL Server
Connection = pymysql.connect(
         host = 'localhost', user = 'root',
         password = '12345', database = 'Demo1')
print("Connection = ", Connection)
# Create the database
cursor = Connection.cursor()
print("Cursor = ", cursor)
#cursor.execute("CREATE DATABASE DEMO1")
#print("Database created successfully!")

# Step 2: Function to create table
def Create_Table_function(cursor,table_name, table_create_declaration):
    create_table_query = f"""CREATE TABLE {table_name} ({table_create_declaration});"""
    print("Create Table Query = ", create_table_query)
    cursor.execute(create_table_query)
    print("Table Created Successfully")
    Connection.commit()

# Step 3: Function to insert data
def Insertion_into_table(cursor, connection, table_name, table_insert_declaration, values_to_be_inserted):
    insert_query = f"INSERT INTO {table_name} {table_insert_declaration}"
    cursor.executemany(insert_query, values_to_be_inserted)
    print("Data Inserted Successfully")
    connection.commit()

 # Step 4: Function to fetch data
def Fetching_from_table(cursor, table_name, column_selector, flag, condition):
    if flag == 'all_rows':
        fetch_query = f"SELECT {column_selector} FROM {table_name};"
    else:
        fetch_query = f"SELECT {column_selector} FROM {table_name} WHERE {condition};"
    print("Fetch Query = ", fetch_query)
    cursor.execute(fetch_query)
    results = cursor.fetchall()
    print("Fetched Results = ", results)
    return len(results)   
# Step 5: Function to order results
def order_by_results(cursor, table_name, column_header, order_by_arrangement):
    order_query = f"SELECT {column_header} FROM {table_name} ORDER BY {order_by_arrangement};"
    print("Order Query = ", order_query)
    cursor.execute(order_query)
    results = cursor.fetchall()
    print("Ordered Results = ", results)

#Create Table Name
Demo_table = 'Demo_table_Student'
# Table schema
#Demo_table_create_declaration = "student_id INT, student_name VARCHAR(50), student_age INT, student_Location VARCHAR(50)"
# Function Call to create the table
#Create_Table_function(cursor, Demo_table, Demo_table_create_declaration)

#Values to be inserted
#Values_to_be_inserted = [(1, 'Alice', 20, 'New York'),(2, 'Bob', 22, 'Los Angeles'),
    #(3, 'Charlie', 21, 'Chicago'), (4, 'David', 23, 'Houston'), (5, 'Eve', 19, 'Phoenix')]
#Demo_table_insert_declaration = "(student_id, student_name, student_age, student_Location) VALUES (%s, %s, %s, %s)"
# Call: Insert data
#Insertion_into_table(cursor, Connection, Demo_table, Demo_table_insert_declaration, Values_to_be_inserted)

#Fetching from table
#column_selector, flag, condition = '*', 'all_rows', 'student_id = 2'
#number_of_records = Fetching_from_table(cursor, Demo_table, column_selector, flag, condition)
#print("Number of Records Fetched = ", number_of_records)

#Order By results
order_by_arrangement,column_header='ASC','student_age'
Fetched_results=order_by_results(cursor, Demo_table, column_header, order_by_arrangement)
print("Ordered Results = ", Fetched_results)




