import pymysql
import pandas as pd

# MySQL connection
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345',
    database='phonepay_db'
)
#2. Device Dominance and User Engagement Analysis

# 1. Query - Underutilized Brands
query1 = """
SELECT State, Brand_type, Registered_count, Percentage_Userdevices
FROM agg_user
WHERE Registered_count > 1000 
AND Percentage_Userdevices < 0.1
ORDER BY Percentage_Userdevices ASC
LIMIT 10
"""
df = pd.read_sql(query1, connection)

# 2. Top 5 Device Brands by Registration in Each State

query2 = """
SELECT Year, Brand_type, SUM(Registered_count) AS Yearly_Registrations
FROM agg_user
GROUP BY Year, Brand_type
ORDER BY Year, Yearly_Registrations DESC
LIMIT 10
"""
df2 = pd.read_sql(query2, connection)

# 3. Trend by Year - Top Brands
query3 = """
SELECT 
Year, 
    Brand_type, 
    SUM(Registered_count) AS Yearly_Registrations
FROM agg_user
GROUP BY Year, Brand_type
ORDER BY Year, Yearly_Registrations DESC
LIMIT 10
"""
df3 = pd.read_sql(query3, connection)

connection.close()

print("Underutilized Brands:\n", df)
print("Top 5 Device Brands by Registration in Each State:\n", df2)
print("Trend by Year - Top Brands:\n", df3)
