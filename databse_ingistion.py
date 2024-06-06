import psycopg2
import csv

# Database connection parameters
dbname = "real_estate_data"
user = "root"
password = "secret"
host = "localhost"
port = "5432"

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

# Open a cursor to perform database operations
cur = conn.cursor()

# Path to your CSV file
csv_file = "real estate data.csv"

# SQL statement to create a table
create_table_query = """
 CREATE TABLE IF NOT EXISTS real_estate( 
    id varchar PRIMARY KEY, 
    title varchar, 
    address varchar, 
    price float, 
    space_sqm varchar, 
    rooms varchar, 
    bathrooms varchar);
"""

# Execute the create table query
cur.execute(create_table_query)
conn.commit()

# Ingest data from CSV into the database
with open(csv_file, 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Skip the header row if present
    for row in reader:
        cur.execute(
            "INSERT INTO real_estate (id, title, address, price, space_sqm, rooms, bathrooms) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            row
        )

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()
