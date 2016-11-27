import csv
import os
import sys
from urllib import urlopen
import json
from bs4 import BeautifulSoup
import requests
all_season_game_id = {}

seasons = []
for i in range(11): #2015-2016 Done!
	current_season = 4+i
	current_season = 200 + current_season
	current_season = "00"+str(current_season)
	seasons.append(current_season) #2015-2016 season
games = range(1230)
#print seasons
#seasons = ["00215"]
#start_ = "15"
start_ = "04"
counter = 0
for season in seasons:
	game_id = []
	start = str(int(start_) + counter)
	if (len(start)) == 1:
		start = "0"+start
	#print start
	for game in games:
		string = str(game+1)
		if (len(string) == 1):
			string = "000"+string
		elif (len(string) == 2):
			string = "00"+string
		elif (len(string) == 3):
			string = "0" + string
		game_id.append(season+"0"+string)
	all_season_game_id[start]=game_id
	counter = counter + 1

#print all_season_game_id

url_prefix = "http://stats.nba.com/stats/playbyplayv2?EndPeriod=10&EndRange=55800&GameID="
url_end = "&RangeType=2&Season=2015-16&SeasonType=Regular+Season&StartPeriod=1&StartRange=0"


headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'}

for season in all_season_game_id:
	all_games_id = all_season_game_id[season]
	if (not os.path.exists(season)):
		os.makedirs(season)
	for game_id in all_games_id:
		result = []
		url = url_prefix+game_id+url_end
		r = requests.get(url,headers=headers)
		content = r.json()
		s = []
		try:
			team1 = content["resultSets"][0]["rowSet"][1][17]
		except:
			print "Index Out of Range: " + game_id
			continue
		team2 = content["resultSets"][0]["rowSet"][1][24] 
		team3 = content["resultSets"][0]["rowSet"][1][31]
		s.append(team1)
		if ((team2 not in s) and team2 != None):
			s.append(team2)
		if ((team3 not in s) and team3 != None):
			s.append(team3)
		try:
			team1 = content["resultSets"][0]["rowSet"][2][17]
		except:
			print "Index Out of Range: " + game_id
			continue
		team2 = content["resultSets"][0]["rowSet"][2][24] 
		team3 = content["resultSets"][0]["rowSet"][2][31]
		s.append(team1)
		if ((team2 not in s) and team2 != None):
			s.append(team2)
		if ((team3 not in s) and team3 != None):
			s.append(team3)
		#print s
		home = s[0]
		if (len(s) == 1):
			away = s[0]
			print "Duplicate Home-Away:" + game_id
		else:
			away = s[1]
		header = content["resultSets"][0]["headers"]
		content = content["resultSets"][0]["rowSet"]
		result.append(header)
		result = result + content
		try:
			file = open(season+"/"+game_id+"_"+home+"_"+away+"_"+".bsv","wb")
			writer = csv.writer(file,delimiter="|")
			for row in result:
				writer.writerow(row)
			file.close()
		except:
			print "Null Team Names: " + game_id 
