#!/usr/bin/env python
import requests
from bs4 import BeautifulSoup
import time  # Import time module for delays
import psycopg2


url_base = 'https://www.365chess.com/'

players = [('Carlsen', 'https://www.365chess.com/search_result.php?wid=8014&bid=&wlname=Carlsen%2C+Magnus&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Anand', 'https://www.365chess.com/search_result.php?wid=3556&bid=&wlname=Anand%2C+Viswanathan&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Kramnik', 'https://www.365chess.com/search_result.php?wid=8218&bid=&wlname=Kramnik%2C+Vladimir&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Kasparov', 'https://www.365chess.com/search_result.php?wid=6404&bid=&wlname=Kasparov%2C+Garry&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Aronian','https://www.365chess.com/search_result.php?wid=3509&bid=&wlname=Aronian%2C+Levon&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Caruana','https://www.365chess.com/search_result.php?wid=9620&bid=&wlname=Caruana%2C+Fabiano&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Nakamura','https://www.365chess.com/search_result.php?wid=8099&bid=&wlname=Nakamura%2C+Hikaru&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('So','https://www.365chess.com/search_result.php?wid=27152&bid=&wlname=So%2C+Wesley&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Karjakin','https://www.365chess.com/search_result.php?wid=&bid=&wlname=Sergey+Karjakin&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Nepomniachtchi','https://www.365chess.com/search_result.php?wid=205373&bid=&wlname=Nepomniachtchi%2C+Ian&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Ding','https://www.365chess.com/search_result.php?wid=166806&bid=&wlname=Ding%2C+Liren&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Giri','https://www.365chess.com/search_result.php?wid=191936&bid=&wlname=Giri%2C+Anish&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Radjabov','https://www.365chess.com/search_result.php?wid=8217&bid=&wlname=Radjabov%2C+Teimour&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Mamedyarov','https://www.365chess.com/search_result.php?wid=9831&bid=&wlname=Mamedyarov%2C+Shakhriyar&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Vachier','https://www.365chess.com/search_result.php?wid=5613&bid=&wlname=Vachier+Lagrave%2C+Maxime&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Gelfand','https://www.365chess.com/search_result.php?wid=&bid=&wlname=Boris+Gelfand&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Svidler','https://www.365chess.com/search_result.php?wid=2690&bid=&wlname=Svidler%2C+Peter&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Topalov','https://www.365chess.com/search_result.php?wid=8221&bid=&wlname=Topalov%2C+Veselin&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Grischuk','https://www.365chess.com/search_result.php?wid=3570&bid=&wlname=Grischuk%2C+Alexander&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Polgar','https://www.365chess.com/search_result.php?wid=8220&bid=&wlname=Polgar%2C+Judit&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Ivanchuk','https://www.365chess.com/search_result.php?wid=3198&bid=&wlname=Ivanchuk%2C+Vassily&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Morozevich','https://www.365chess.com/search_result.php?wid=8226&bid=&wlname=Morozevich%2C+Alexander&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Dominguez','https://www.365chess.com/search_result.php?wid=208977&bid=&wlname=Dominguez+Perez%2C+Leinier&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Rapport','https://www.365chess.com/search_result.php?wid=&bid=&wlname=Rapport&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Dubov','https://www.365chess.com/search_result.php?wid=203091&bid=&wlname=Dubov%2C+Daniil&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Abasov','https://www.365chess.com/search_result.php?wid=193816&bid=&wlname=Abasov%2C+Nijat&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Praggnanandhaa','https://www.365chess.com/search_result.php?wid=217858&bid=&wlname=Praggnanandhaa+R&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Gukesh','https://www.365chess.com/search_result.php?wid=219024&bid=&wlname=Gukesh+D&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Firouzja','https://www.365chess.com/search_result.php?wid=217913&bid=&wlname=Firouzja%2C+Alireza&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Duda','https://www.365chess.com/search_result.php?wid=173496&bid=&wlname=Duda%2C+Jan+Krzysztof&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Navara','https://www.365chess.com/search_result.php?wid=2460&bid=&wlname=Navara%2C+David&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Adams','https://www.365chess.com/search_result.php?wid=3508&bid=&wlname=Adams%2C+Michael&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Kamsky','https://www.365chess.com/search_result.php?wid=88881&bid=&wlname=Kamsky%2C+Gata&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Abdusattorov','https://www.365chess.com/search_result.php?wid=217283&bid=&wlname=Abdusattorov%2C+Nodirbek&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Wei','https://www.365chess.com/search_result.php?wid=215280&bid=&wlname=Wei%2C+Yi&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Saric','https://www.365chess.com/search_result.php?wid=20967&bid=&wlname=Saric%2C+Ivan&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Vidit','https://www.365chess.com/search_result.php?wid=198074&bid=&wlname=Vidit%2C+Santosh+Gujrathi&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Erigaisi','https://www.365chess.com/search_result.php?wid=218143&bid=&wlname=Erigaisi+Arjun&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Tari','https://www.365chess.com/search_result.php?wid=201470&bid=&wlname=Tari%2C+Aryan&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Nihal','https://www.365chess.com/search_result.php?wid=217855&bid=&wlname=Nihal+Sarin&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Van Foreest','https://www.365chess.com/search_result.php?wid=215705&bid=&wlname=Van+Foreest%2C+Jorden&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Leko','https://www.365chess.com/search_result.php?wid=10836&bid=&wlname=Leko%2C+Peter&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Tomashevsky','https://www.365chess.com/search_result.php?wid=11583&bid=&wlname=Tomashevsky%2C+Evgeny&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Le, Q','https://www.365chess.com/search_result.php?wid=6891&bid=&wlname=Le%2C+Quang+Liem&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Artemiev','https://www.365chess.com/search_result.php?wid=215394&bid=&wlname=Artemiev%2C+Vladislav&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1'),
        ('Alekseenko','https://www.365chess.com/search_result.php?wid=212200&bid=&wlname=Alekseenko%2C+Kirill&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1')]
        
connection_string = "postgres://tsdbadmin:m2qyg360nit0cecm@i9kfhxvrde.y7udgt23ov.tsdb.cloud.timescale.com:30616/tsdb?sslmode=require"
conn = psycopg2.connect(connection_string)


cur = conn.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS chess_games (
    id SERIAL PRIMARY KEY,
    player1_name text,
    player1_elo text,
    player2_name text,
    player2_elo text,
    result text,
    date text,
    event_name text,
    event_type text,
    event_format text,
    player1_points INTEGER,
    player2_points INTEGER
);
"""

# Execute the create table query
cur.execute(create_table_query)

# Commit the transaction to apply changes
conn.commit()
urls = [('Carlsen', 'https://www.365chess.com/search_result.php?wid=8014&bid=&wlname=Carlsen%2C+Magnus&open=&blname=&eco=&nocolor=on&yeari=&yeare=&sply=1&ply=&res=&submit_search=1')]

for name, url in players:

    games = []
    i = 0
    while url:
        i = i + 1
        # Send a GET request to the URL
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to retrieve page: {url}")
            break
        html_content = response.text

        # Parse the HTML content
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find all <tr> elements
        tr_elements = soup.find_all('tr')

        # Extract game information from each <tr> element
        for tr in tr_elements:
            tds = tr.find_all('td')
            date_td = tr.find('td', id='col-dat')
            if date_td:
                date = date_td.text.strip()
            else:
                date = "Date Not Found"

            event_name = "Event Not Found"
            event_type = 'Other'
            event_format = 'Classical'

            event_name_tag = tr.find('a', href=lambda href: href and 'tournaments' in href)
            if event_name_tag:
                event_name = event_name_tag.text.strip()
                event_type = 'Tournament'
                if 'Blitz' in event_name:
                    event_format = 'Blitz'
                elif 'Rapid' in event_name:
                    event_format = 'Rapid'
                elif 'Bullet' in event_name:
                    event_format = 'Bullet'

            welo_td = tr.find('td', id='col-welo')
            if welo_td:
                welo = welo_td.text.strip()
            else:
                    welo = "welo Not Found"

            belo_td = tr.find('td', id='col-belo')
            if belo_td:
                belo = belo_td.text.strip()
            else:
                    belo = "belo Not Found"

            if len(tds) >= 7:
                player1_name = tds[0].text.strip()
                player1_elo = welo
                player2_name = tds[2].text.strip()
                player2_elo = belo
                result = tds[4].text.strip().split(' ')[0]

                

                player1_points = 0
                player2_points = 0
                if '-' in result:
                    player1_points, player2_points = result.split('-')
                    if player1_points=='½' or player2_points=='½':
                        player1_points = 1
                        player2_points = 1
                    else:
                        player1_points = float(player1_points) * 2
                        player2_points = float(player2_points) * 2



                game_info = {
                    'Player 1': player1_name,
                    'Player 1 ELO': player1_elo,
                    'Player 2': player2_name,
                    'Player 2 ELO': player2_elo,
                    'Result': result,
                    'Date': date,
                    'Event': event_name,
                    "Event Type": event_type,
                    "Event Format": event_format,
                    "Player 1 Points": player1_points,
                    "Player 2 Points": player2_points
                }
                games.append(game_info)
        print(games)
        print(i)
        # Delay for 5 seconds to avoid overloading the website
        time.sleep(5)

        # Find next page link
        nav_element = soup.find('nav', attrs={'aria-label': 'Page navigation'})
        if nav_element:
            next_link = nav_element.find('a', attrs={'aria-label': 'Next'})
            if next_link:
                url = url_base + next_link['href']
            else:
                url = None
        else:
            url = None

    # Print all game information
    for i, game in enumerate(games):
        if i == 0:
            continue
        print(game)
        player1_name = game['Player 1']
        player1_elo = game['Player 1 ELO']
        player2_name = game['Player 2']
        player2_elo = game['Player 2 ELO']
        result = game['Result']
        date = game['Date']
        event_name = game['Event']
        event_type = game['Event Type']
        event_format = game['Event Format']
        player1_points = game['Player 1 Points']
        player2_points = game['Player 2 Points']

        insert_query = """
            INSERT INTO chess_games 
            (player1_name, player1_elo, player2_name, player2_elo, result, date, event_name, event_type, event_format, player1_points, player2_points)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """

        cur.execute(insert_query, (player1_name, player1_elo, player2_name, player2_elo, result, date, event_name, event_type, event_format, player1_points, player2_points))

    conn.commit()


    print(len(games))


print('HELLO')

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