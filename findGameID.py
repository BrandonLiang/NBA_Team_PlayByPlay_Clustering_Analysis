import csv
import sys
from urllib import urlopen
import json
from bs4 import BeautifulSoup
import requests

url_prefix = "http://stats.nba.com/stats/playbyplayv2?EndPeriod=10&EndRange=55800&GameID="
#0041500223
url_end = "&RangeType=2&Season=2015-16&SeasonType=Regular+Season&StartPeriod=1&StartRange=0"

number = 41200000
gap = 100000
gameID_prefix = "00"
#gameIDFile = open("game_id.csv","wb")
#gameID_csv = csv.writer(gameIDFile)
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'}

while (True):
	counter = 0
	while (counter < 500):
		#print number
		url = url_prefix+gameID_prefix+str(number)+url_end

		#content = urlopen(url)
		#print url
		#print json.load(content.fp) 
		r = requests.get(url,headers=headers)
		#try:
		try:
			#r.json()
			content = r.json()
			content = content["resultSets"][0]["rowSet"]
			if len(content)>0:
				print gameID_prefix+str(number)
				#gameID_csv.writerow(gameID_prefix+str(number))
		except Exception, e:
			number = number
	
		#except Exception, e:
		
		#continue
		number = number + 1
		counter = counter + 1
	#break
	#if (len(content.read())>1):
		#store.append([gameID_prefix+str(number)])
		#print content.read()
	#print number, "yes"
	
	number = number - 499 - gap
	
# Close file stream!
#gameIDFile.close()