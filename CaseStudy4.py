import pymysql
import pandas as pd

# Connect to MySQL
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345',
    database='phonepay_db'
)

#9. Insurance Transactions Analysis
# 1. Top States by Insurance Transaction Amount
query1 = f"""
SELECT State,SUM(User_amount) AS Total_Amount,SUM(User_count) AS Total_Users
FROM map_insurance
WHERE Year = year AND Quarter = quarter
GROUP BY State
ORDER BY Total_Amount DESC
LIMIT 10
"""
df1 = pd.read_sql(query1, connection)

# 2. Top Districts by Insurance Transaction Volume
query2 = f"""
SELECT State,District,SUM(User_count) AS Total_Users,SUM(User_amount) AS Total_Amount
FROM map_insurance
WHERE Year = year AND Quarter = quarter
GROUP BY State, District
ORDER BY Total_Users DESC
LIMIT 10
"""
df2 = pd.read_sql(query2, connection)

# 3. Insurance Quarterly Trends by State
query3 = """
SELECT State,Year,Quarter,SUM(User_amount) AS Total_Amount,SUM(User_count) AS Total_Users
FROM map_insurance
GROUP BY State, Year, Quarter
ORDER BY State, Year, Quarter
LIMIT 10
"""
df3 = pd.read_sql(query3, connection)

# 4. Districts with Low Insurance Activity (Threshold < 1000 users)
query4 = f"""SELECT State,District, SUM(User_count) AS Total_Users,SUM(User_amount) AS Total_Amount
FROM map_insurance
WHERE Year = year AND Quarter = quarter
GROUP BY State, District
HAVING Total_Users < 1000
ORDER BY Total_Users ASC
LIMIT 10
"""
df4 = pd.read_sql(query4, connection)

# 5. Year-over-Year Insurance Growth by State
query5 = """
SELECT State,Year,SUM(User_count) AS Yearly_Users,SUM(User_amount) AS Yearly_Amount
FROM map_insurance
GROUP BY State, Year
ORDER BY State, Year
LIMIT 10
"""
df5 = pd.read_sql(query5, connection)

# === Close the Connection ===
connection.close()

# Print Results
print("\n Top 10 States by Insurance Transaction Amount:")
print(df1)

print("\n Top 10 Districts by Insurance Transaction Volume:")
print(df2)

print("\n Insurance Quarterly Trends by State:")
print(df3)

print("\n Districts with Low Insurance Activity:")
print(df4)

print("\n Year-over-Year Insurance Growth by State:")
print(df5)