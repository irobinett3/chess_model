#!/usr/bin/env python
import pandas as pd

csv_file = 'chess_games.csv'


df = pd.read_csv(csv_file)

df.rename(columns={'points': 'player1_points'}, inplace=True)
df['player2_points'] = 2 - df['player1_points']

for index, row in df.iterrows():
    if index < 14491:
        if 'Carlsen' not in row['player1_name']:
            temp = row['player1_name']
            df.at[index, 'player1_name'] = row['player2_name']
            df.at[index, 'player2_name'] = temp

# Save the modified DataFrame back to CSV
modified_csv_file = 'chess_games.csv'
df.to_csv(modified_csv_file, index=False)

print(f"Modified CSV saved to {modified_csv_file}")