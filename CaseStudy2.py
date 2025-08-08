import pymysql
import pandas as pd

# Connect to MySQL
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345',
    database='phonepay_db'
)
# 6.Analyze insurance-related transactions
# 1. df1 - Total Insurance Users and Amount by State
query1 = """
SELECT 
    State,
    SUM(User_count) AS Total_Users,
    SUM(User_amount) AS Total_Amount
FROM map_transaction
GROUP BY State
ORDER BY Total_Amount DESC;"""
df1 = pd.read_sql(query1, connection)

# 2. df2 - Year-wise Insurance Uptake by State
query2 = """
SELECT 
    State,
    Year,
    SUM(User_count) AS Yearly_Users,
    SUM(User_amount) AS Yearly_Amount
FROM map_transaction
GROUP BY State, Year
ORDER BY State, Year
LIMIT 10;
"""
df2 = pd.read_sql(query2, connection)

# 3. df3 - Quarterly Insurance Trend by State
query3 = """
SELECT 
    State,
    Year,
    Quarter,
    SUM(User_count) AS Quarterly_Users,
    SUM(User_amount) AS Quarterly_Amount
FROM map_transaction
GROUP BY State, Year, Quarter
ORDER BY State, Year, Quarter
LIMIT 10;
"""
df3 = pd.read_sql(query3, connection)

# 4. df4 - District-wise Insurance Uptake for All States
query4 = """
SELECT 
    State,
    District,
    SUM(User_count) AS Total_Users,
    SUM(User_amount) AS Total_Amount
FROM map_transaction
GROUP BY State, District
ORDER BY State, Total_Amount DESC
LIMIT 10;
"""
df4 = pd.read_sql(query4, connection)

# 5. df5 - Districts with Low Insurance Activity (User_count < 1000)
query5 = """
SELECT 
    State,
    District,
    SUM(User_count) AS Total_Users,
    SUM(User_amount) AS Total_Amount
FROM map_transaction
GROUP BY State, District
HAVING SUM(User_count) < 10000
ORDER BY Total_Users ASC
LIMIT 10;
"""
df5 = pd.read_sql(query5, connection)

# Close the connection
connection.close()

# Print sample outputs
print(" df1 - Total by State:\n", df1.head())
print(" df2 - Year-wise by State:\n", df2.head())
print(" df3 - Quarter-wise by State:\n", df3.head())
print(" df4 - District-wise Details:\n", df4.head())
print(" df5 - Low Activity Districts:\n", df5.head())
