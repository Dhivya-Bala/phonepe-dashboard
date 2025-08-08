import pymysql
import pandas as pd

# Connect to MySQL
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='12345',
    database='phonepay_db'
)
#Group by State and Quarter – Total Transaction Amount
query1 = """
    SELECT State,Year,Quarter,SUM(Transaction_amount) AS Total_Amount    
    FROM agg_transaction
    GROUP BY State, Year, Quarter
    ORDER BY Total_Amount DESC
    LIMIT 10
"""
# Execute SQL query using pandas
df = pd.read_sql(query1, connection)
print("Quarter-wise Aggregated by State, Year, Quarter:")
print(df)

# Group by State and Year – Total Transactions
query2 = """
SELECT State, Year, 
       SUM(Transaction_count) AS Yearly_Transactions,
       SUM(Transaction_amount) AS Yearly_Amount
FROM agg_transaction
WHERE Year = 2023
GROUP BY State, Year
ORDER BY State, Year
LIMIT 10
"""
df2 = pd.read_sql(query2, connection)
print("Year-wise Aggregated by State, Year:")
print(df2)

#  Group by Transaction Type – Overall Category Trends
query3 = """
SELECT Transaction_type, SUM(Transaction_count) AS Total_Transactions
FROM agg_transaction
WHERE Quarter = 1 AND Year = 2023
GROUP BY Transaction_type
ORDER BY Total_Transactions ASC
LIMIT 5
"""
df3 = pd.read_sql(query3, connection)
print("Overall Category Trends- Transaction Type:")
print(df3)


# 4. Quarters Showing Decline or Stagnation (Optional Analysis)
query4 = """
SELECT State,Year,Quarter,
SUM(Transaction_amount) AS Total_Amount
FROM agg_transaction
GROUP BY State, Year, Quarter
HAVING SUM(Transaction_amount) < 100000
ORDER BY Total_Amount ASC
LIMIT 4
"""
df4 = pd.read_sql(query4, connection)
print("Quarters Showing Decline or Stagnation:")
print(df4)

# 5. Quarters Showing Decline or Stagnation (Optional Analysis)
query5 = """
SELECT State,Year,Quarter,
SUM(Transaction_amount) AS Total_Amount
FROM agg_transaction
GROUP BY State, Year, Quarter
HAVING SUM(Transaction_amount) < 100000
ORDER BY Total_Amount ASC
LIMIT 4
"""
df5 = pd.read_sql(query5, connection)
print("Quarters Showing Decline or Stagnation:")
print(df5)

# Close the connection
connection.close()
