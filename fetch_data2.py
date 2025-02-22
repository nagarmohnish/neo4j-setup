import psycopg2
import pandas as pd

# 1. Database connection details
dbname = "social_data"  
user = "safi"          
password = "safi"      
host = "34.55.135.250" 
port = "5432"          

# 2. Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

# 3. Write your JOIN query
query = """
    SELECT 
        c.id AS tweet_id,
        c.content AS post_content,
        c.datetime AS post_datetime,
        c.likes,
        c.shares,
        c.views,
        c.username AS post_username,
        c.hashtags,
        c.created_on AS post_created_on,
        
        com.tweet_id AS comment_id,
        com.content,
        com.created_on AS comment_datetime
    FROM public.corporate c
    LEFT JOIN public.corporate_comments com
    ON c.id = com.tweet_id
    LIMIT 100
"""

# 4. Fetch data into a Pandas DataFrame
df = pd.read_sql(query, conn)

# 5. Close the connection
conn.close()

# 6. Explore your DataFrame
print(df.head())       # Preview the first 5 rows
print(df.columns)      # List of column names
print(df.info())       # Summary: data types, non-null counts, etc.
