import pymysql
import pandas as pd

# Connect to MySQL
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345',
    database='phonepay_db'
)

#7. Transaction Analysis Across States and Districts

# 1. df1 - Top 10 States by Transaction Value
query1 = """
SELECT State,SUM(User_amount) AS Total_Amount,SUM(User_count) AS Total_Users
FROM map_transaction
GROUP BY State
ORDER BY Total_Amount DESC
LIMIT 10
"""
df1 = pd.read_sql(query1, connection)

# 2. df2 - Top 10 Districts by Transaction Volume
query2 = """
SELECT State,District,SUM(User_count) AS Total_Users,SUM(User_amount) AS Total_Amount
FROM map_transaction
GROUP BY State, District
ORDER BY Total_Users DESC
LIMIT 10
"""
df2 = pd.read_sql(query2, connection)

# 3. df3 - Quarterly Trend in High-performing States (User_amount > 10 million)
query3 = """
SELECT State,Year,Quarter,SUM(User_count) AS Total_Users,SUM(User_amount) AS Total_Amount
FROM map_transaction
GROUP BY State, Year, Quarter
HAVING SUM(User_amount) > 10000000
ORDER BY Total_Amount DESC
Limit 10
"""
df3 = pd.read_sql(query3, connection)

# 4. df4 - Districts with Low Transaction Activity (User_count < 1000)
query4 = """
SELECT State,District,SUM(User_count) AS Total_Users,SUM(User_amount) AS Total_Amount
FROM map_transaction
GROUP BY State, District
HAVING SUM(User_count) < 1000
ORDER BY Total_Users ASC
LIMIT 10
"""
df4 = pd.read_sql(query4, connection)

# 5. df5 - Year-over-Year Growth Trend by State
query5 = """
SELECT State,Year,SUM(User_count) AS Yearly_Users,SUM(User_amount) AS Yearly_Amount
FROM map_transaction
GROUP BY State, Year
ORDER BY State, Year
LIMIT 10
"""
df5 = pd.read_sql(query5, connection)

# Close the connection
connection.close()

# Optional: print sample results
print(" df1 - Top 10 States by Transaction Value:\n", df1.head())
print(" df2 - Top 10 Districts by Transaction Volume:\n", df2.head())
print(" df3 - Quarterly Trend in High-performing States:\n", df3.head())
print(" df4 - Districts with Low Transaction Activity:\n", df4.head())
print(" df5 - Year-over-Year Growth Trend:\n", df5.head())
