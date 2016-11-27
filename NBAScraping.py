from urllib import urlopen
from bs4 import BeautifulSoup
import pandas as pd

season = "00215" #2015-2016 season
games = range(1230)

game_id = []
for game in games:
	game_id.append(season+"0"+str(game+1))

print game_id

url = "http://www.basketball-reference.com/draft/NBA_2014.html"

html = urlopen(url)
soup = BeautifulSoup(html)

column_headers = [th.getText() for th in soup.findAll('tr',limit=2)[1].findAll('th')]
#print len(column_headers)
#print column_headers
column_headers.remove('Pk')

data_rows = soup.findAll('tr')[2:]
player_data = [[td.getText() for td in data_rows[i].findAll('td')] for i in range(len(data_rows))]
#print len(player_data[0])
#print player_data[0]
df = pd.DataFrame(player_data, columns=column_headers)

df = df[df.Player.notnull()]

print df.head()

print column_headers