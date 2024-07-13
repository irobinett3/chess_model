#!/usr/bin/env python
import requests
from bs4 import BeautifulSoup
import time  # Import time module for delays
import psycopg2


# Database connection string
connection_string = "postgres://tsdbadmin:m2qyg360nit0cecm@i9kfhxvrde.y7udgt23ov.tsdb.cloud.timescale.com:30616/tsdb?sslmode=require"
conn = psycopg2.connect(connection_string)
cur = conn.cursor()

# SQL query to remove the last two rows
remove_last_two_rows_query = """
SELECT * from chess_games;
"""
cur.execute(remove_last_two_rows_query)

# Commit the transaction (since you made changes to the database)
conn.commit()

# Execute the query to remove the last two rows


# Closing the cursor and connection
cur.close()
conn.close()
