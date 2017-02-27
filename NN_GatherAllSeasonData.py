import os
import csv
import sys
import json

start = int(sys.argv[1])
end = int(sys.argv[2])

set_ = []
set_ready = []
sample_dir = "/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation/4_Game_BreakDown_Summary/"
sample_team = "76ers_Game_BreakDown_Summary.csv"

file_ = open(sample_dir+sample_team, "rb")
reader = csv.reader(file_)

header = reader.next()
file_.close()

root = "/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation/"
folder = "_Game_BreakDown_Summary/"

dic = {}

with open("/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation/clean_records.json") as json_file:
  json_dict_season = json.load(json_file)

  json_file.close()
# print json_dict_season

def eachSeason(season):
  path = root + str(season) + folder
  for filename in os.listdir(path):
    if (filename != ".DN_Store"):
      print str(season) + "-" + filename
      file_ = open(path + filename, "rb")
      reader = csv.reader(file_)
      reader.next()
      team1 = filename.split("_")[0]
      for line in reader:
        info = line[0]
        gameID = info.split("_")[0]
        team2 = info.split("_")[-1]
        for game in json_dict_season[str(season)][team1][team2]:
          #Only want to include home games
          if (game["game_id"] == gameID and game["home"] == "1"):
            line[0] = gameID + "_" + team1 + "_host_" + team2
            set_.append(line)
            set_ready.append(line[1:])
      file_.close()

current = start
while (current <= end):
  eachSeason(current)
  current = current + 1


filepath = "NN_Each_Game_Distribution_" + str(start) + "_" + str(end) + ".csv"
file_ = open(filepath,"wb")
writer = csv.writer(file_)
writer.writerow(header)
for line in set_:
  writer.writerow(line)

file_.close()

filepath = "NN_Each_Game_Distribution_" + str(start) + "_" + str(end) + "_ready.csv"
file_ = open(filepath,"wb")
writer = csv.writer(file_)
for line in set_ready:
  writer.writerow(line)

file_.close()
