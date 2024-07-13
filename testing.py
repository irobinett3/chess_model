#!/usr/bin/env python
import requests
from bs4 import BeautifulSoup
import time  # Import time module for delays
import psycopg2


url_base = 'https://www.365chess.com/'
url = 'https://www.365chess.com/search_result.php?wid=8014&bid=&wlname=Carlsen%2C+Magnus&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'


connection_string = "postgres://tsdbadmin:m2qyg360nit0cecm@i9kfhxvrde.y7udgt23ov.tsdb.cloud.timescale.com:30616/tsdb?sslmode=require"
conn = psycopg2.connect(connection_string)


cur = conn.cursor()

print_query="""
SELECT * from chess_games
"""
cur.execute(print_query)
fetched = cur.fetchall()
for row in fetched:
    print(row)

cur.close()
conn.close()
print(len(fetched))
print("HI")