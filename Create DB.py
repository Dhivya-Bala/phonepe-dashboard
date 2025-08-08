import pymysql

# Step 1: Connect to MySQL Server (No DB specified yet)
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345'  # Replace with your MySQL password
)

# Step 2: Create a cursor
cursor = connection.cursor()

# Step 3: Create a new database
cursor.execute("CREATE DATABASE IF NOT EXISTS StudentDB")
print("Database 'StudentDB' created successfully!")

# Step 4: Close connection
cursor.close()
connection.close()
