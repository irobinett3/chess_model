#!/usr/bin/env python
import requests
from bs4 import BeautifulSoup
import json
import re


url = 'https://www.365chess.com/search_result.php?wid=3556&bid=&wlname=Anand%2C+Viswanathan&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'
# Send a GET request to the URL

response = requests.get(url)
html_content = response.text
print(html_content)
# Parse the HTML content
soup = BeautifulSoup(html_content, 'html.parser')

# Find all <a> tags (links)
links = soup.find_all('a')

tr_elements = soup.find_all('tr')

    # Extract game information from each <tr> element
for tr in tr_elements:
    tds = tr.find_all('td')
    date_td = tr.find('td', id='col-dat')
    if date_td:
        date = date_td.text.strip()
    else:
        date = "Date Not Found"