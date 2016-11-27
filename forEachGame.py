import os
import sys
import csv

import pandas as pd 


def determine_home_away(game_file):
	teams = game_file.split("_")[1:3]
	id_ = game_file.split("_")[0]
	file = open(game_file,"rb")
	reader = csv.reader(file,delimiter="|")
	reader.next()

	for line in reader:
		score = line[10]
		margin = line[11]
		team = line[17]
		if (score != ""):
			margin = int(margin)
			teams.remove(team)
			if (margin > 0):
				# home team scores first
				home = team
				away = teams[0]
			else:
				away = team
				home = teams[0]
			break

	#print "Home:", home, "Away:", away
	file.close()
	return (id_,home,away)

#determine_home_away("0021501228_Lakers_Jazz_.bsv")
#determine_home_away("0021500003_Warriors_Pelicans_.bsv")



def change_file_name(season): 
	for root, dirs, files in os.walk(season):
		for f in files:
			if f.endswith(".bsv"):
				(id_,home,away) = determine_home_away(f)
				os.rename(f,id_+"_"+home+"_"+away+".bsv")

#change_file_name()

def collect_all_teams(season):
	teams = []
	for root, dirs, files in os.walk(season):
		for f in files:
			if (("-" not in f) and (".swp" not in f) and f.endswith(".bsv")):
				home = f.split("_")[1]
				if (home not in teams):
					teams.append(home)
	return teams

def all_game_plays_for_team(season_,team):
	log_all = [["Game ID","Opponent","Home/Away","Time Left","Offense/Defense","Player","Player ID","Score_Before_This Play","Margin_Before_This Play","Description_Play","Score_After_This Play","Margin_After This Play"]]
	log_offense = [["Game ID","Opponent","Home/Away","Time Left","Player","Player ID","Score_Before_This Play","Margin_Before_This Play","Description_Play","Score_After_This Play","Margin_After This Play"]]
	log_defense = [["Game ID","Opponent","Home/Away","Time Left","Player","Player ID","Score_Before_This Play","Margin_Before_This Play","Description_Play","Score_After_This Play","Margin_After This Play"]]
	isHome = "1"
	season = season_
	if season_[0] == "0":
		season = season_[1]
	for root, dirs, files in os.walk(season_):
		for f in files:
			if ((team in f) and ("-" not in f)):
				id_ = f.split("_")[0]
				teams = f.split(".")[0].split("_")[1:3]
				print teams, f
				teams.remove(team)
				opponent = teams[0]
				if (f.split("_")[1] == team):
					isHome = "1"
				else:
					isHome = "0"
				entry = [id_,opponent,isHome]
				file_ = open(season_+"/"+f,"rb")
				reader = csv.reader(file_,delimiter="|")
				prev_score = "0--0"
				prev_margin = "0"
				reader.next()
				reader.next()
				reader.next() # omit jump ball logs
				for line in reader:
					entry_copy_all = entry
					entry_copy_offense = entry
					entry_copy_defense = entry
					quarter = line[4]
					time_left_in_quarter = line[6]
					minute = time_left_in_quarter.split(":")[0]
					second = time_left_in_quarter.split(":")[1]
					if (int(quarter) <= 4):
						time_left = str((4-int(quarter)) * 12 + int(minute))+":"+second
					else:
						time_left = "-"+time_left_in_quarter
					current_team = line[17]
					if (line[7] == ""):
						description = line[9]
					else:
						description = line[7]
					#print description
					if (current_team == team):
						isOffense = "1"
						#description = line[7]
					else:
						isOffense = "0"
						#description = line[9]
					player = line[14]
					player_id = line[13]
					current_score = line[10].replace(" ","").replace("-","--")
					#current_score_ = line[10].replace(" ","")
					current_margin = line[11]
					if (current_margin == "TIE"):
						current_margin = "0"
					#current_score_ = current_score
					if (isHome == "1"):
						if (current_score != ""):
							scores = current_score.replace(" ","").split("--")
							away_score = scores[0]
							home_score = scores[1]
							current_score = home_score+"--"+away_score
						else:
							current_score = prev_score
							current_margin = prev_margin
					else:
						if (current_score != ""):
							current_margin = str(int(current_margin) * (-1))
						else:
							current_score = prev_score
							current_margin = prev_margin
					entry_copy_all = entry_copy_all + [time_left,isOffense,player,player_id,prev_score,prev_margin,description,current_score,current_margin]
					log_all.append(entry_copy_all)
					if (isOffense == "1"):
						entry_copy_offense = entry_copy_offense + [time_left,player,player_id,prev_score,prev_margin,description,current_score,current_margin]
						log_offense.append(entry_copy_offense)
					else:
						entry_copy_defense = entry_copy_defense + [time_left,player,player_id,prev_score,prev_margin,description,current_score,current_margin]
						log_defense.append(entry_copy_defense)
					if (current_score != ""):
						prev_score = current_score
						prev_margin = current_margin
				file_.close()
  	all_ = open("/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation/"+season+"_Team_Summary/1_"+team+"-All Plays.csv","wb")
	writer_all = csv.writer(all_)
	for row in log_all:
		writer_all.writerow(row)
	all_.close()

	all_offense = open("/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation/"+season+"_Team_Summary/2_"+team+"-All Offensive Plays.csv","wb")
	writer_offense = csv.writer(all_offense)
	for row in log_offense:
		writer_offense.writerow(row)
	all_offense.close()

	all_defense = open("/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation/"+season+"_Team_Summary/3_"+team+"-All Defensive Plays.csv","wb")
	writer_defense = csv.writer(all_defense)
	for row in log_defense:
		writer_defense.writerow(row)
	all_defense.close()

season = sys.argv[1]
if len(season) == 1:
	season = "0"+season
season_ = season
if (season[0] == "0"):
	season_ = season[1]
os.makedirs(season_+"_Team_Summary")

teams = collect_all_teams(season)
for team in teams:
	all_game_plays_for_team(season,team)
