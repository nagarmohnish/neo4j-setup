from neo4j import GraphDatabase
import psycopg2
import pandas as pd

# PostgreSQL Connection
pg_conn = psycopg2.connect(
    dbname="social_data",
    user="safi",
    password="safi",
    host="34.55.135.250",
    port="5432"
)
pg_cursor = pg_conn.cursor()

# Neo4j Connection
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Fetch only 1000 rows from PostgreSQL
query = """
SELECT id, content, datetime, likes, shares, views, username, followers, location, isblue, hashtags, media_url 
FROM public.corporate 
LIMIT 1000
"""
df = pd.read_sql(query, pg_conn)

# Convert DataFrame to List of Dicts
batch_data = df.to_dict(orient="records")

# Function to insert data into Neo4j using our minimal graph model
def insert_data(tx, data):
    cypher_query = """
    UNWIND $data AS row
    // Create or merge the User node
    MERGE (u:User {username: row.username})
        ON CREATE SET 
            u.followers = row.followers, 
            u.location = row.location, 
            u.isblue = row.isblue

    // Create or merge the Post node (renamed from Tweet to Post)
    MERGE (p:Post {id: row.id})
        ON CREATE SET 
            p.content = row.content, 
            p.datetime = row.datetime, 
            p.likes = row.likes, 
            p.shares = row.shares, 
            p.views = row.views

    // Connect the User to the Post
    MERGE (u)-[:POSTED]->(p)
    
    // Create or merge Hashtag nodes and connect them
    FOREACH (hashtag IN row.hashtags | 
        MERGE (h:Hashtag {name: hashtag}) 
        MERGE (p)-[:HAS_HASHTAG]->(h)
    )
    
    // Create or merge Media nodes and connect them
    FOREACH (media IN row.media_url | 
        MERGE (m:Media {url: media}) 
        MERGE (p)-[:HAS_MEDIA]->(m)
    )
    """
    tx.run(cypher_query, data=data)

# Insert Data into Neo4j
with neo4j_driver.session() as session:
    session.write_transaction(insert_data, batch_data)

# Close Connections
pg_cursor.close()
pg_conn.close()
neo4j_driver.close()

print("Inserted 1000 rows successfully!")
