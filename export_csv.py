#!/usr/bin/env python

import psycopg2
import csv

# Database connection details
connection_string = "[Hidden]"
# Establish a connection to the database
conn = psycopg2.connect(connection_string)

# Create a cursor object
cur = conn.cursor()

# Query to select all data from chess_games
query = "SELECT * FROM chess_games"

# Execute the query
cur.execute(query)

# Fetch all rows
rows = cur.fetchall()

# Write data to CSV file
with open('chess_games.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow([desc[0] for desc in cur.description])  # Write header
    csvwriter.writerows(rows)  # Write rows of data

# Close cursor and connection
cur.close()
conn.close()
