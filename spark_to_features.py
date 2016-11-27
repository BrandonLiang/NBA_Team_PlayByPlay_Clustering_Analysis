import csv
import os
import math
import matplotlib.pyplot as plt
import numpy as np

directory = "/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation"

#season range
season_start = 4
season_end = 15

def process_each_file(filename,team,season):
	row = [season+"-"+team]
	file_ = open(filename,"rb")
	reader = csv.reader(file_,delimiter="|")
	total_count = 0
	margin_count = {}
	change_count = {}
	margin_list = [0,0]
	for line in reader:
		before = int(line[0])
		after = int(line[1])
		change = after - before
		count = int(line[2])
		total_count = total_count + count
		if change > 0:
			change_ = str(before) + "_+"
			if (change_ not in change_count):
				change_count[change_] = count
			else:
				temp = change_count[change_]
				temp = temp + count
				change_count[change_] = temp
			margin_list[0] = margin_list[0] + count
		if change < 0:
			change_ = str(before) + "_-"
			if (change_ not in change_count):
				change_count[change_] = count
			else:
				temp = change_count[change_]
				temp = temp + count
				change_count[change_] = temp
			margin_list[1] = margin_list[1] + count
		if (before not in margin_count):
			margin_count[before] = count
		else:
			temp = margin_count[before]
			temp = temp + count
			margin_count[before] = temp
	margin_count_p = {}
	for key in margin_count:
		margin_count_p[key] = 100.0 * margin_count[key] / total_count
	change_count_p = {}
	for key in change_count:
		change_count_p[key] = 100.0 * change_count[key] / total_count
	# print margin_count_p
	# print change_count_p
	# print margin_list

	final_position = {}
	final_margin = {}
	margin_list[0] = 100.0 * margin_list[0] / total_count
	margin_list[1] = 100.0 * margin_list[1] / total_count

	final_position["<= -40"] = 0.0
	final_position["> 40"] = 0.0
	final_margin["<= -40 +"] = 0.0
	final_margin["<= -40 -"] = 0.0
	final_margin["> 40 +"] = 0.0
	final_margin["> 40 -"] = 0.0
	for i in range(16):
		b = i * 5 - 40
		a = b + 5
		final_position[str(b)+" < x <= "+str(a)] = 0.0
		final_margin[str(b) + " < x <= " + str(a) + " +"] = 0.0
		final_margin[str(b) + " < x <= " + str(a) + " -"] = 0.0
	for key in margin_count_p:
		p = margin_count_p[key]
		if key <= -40:
			final_position["<= -40"] = final_position["<= -40"] + p
		elif key > 40:
			final_position["> 40"] = final_position["> 40"] + p
		elif key == 0:
			final_position["-5 < x <= 0"] = final_position["-5 < x <= 0"] + p
		elif key % 5 != 0:
			division = key * 1.0 / 5
			up = int(5 * math.ceil(division))
			down = int(5 * math.floor(division))
			final_position[str(down)+" < x <= "+str(up)] = final_position[str(down)+" < x <= "+str(up)] + p
		else:
			up = key
			down = key - 5
			final_position[str(down)+" < x <= "+str(up)] = final_position[str(down)+" < x <= "+str(up)] + p

	for key in change_count_p:
		p = change_count_p[key]
		key_l = key.split("_")
		margin = int(key_l[0])
		sign = key_l[1]
		if margin <= -40:
			final_margin["<= -40 " + sign] = final_margin["<= -40 " + sign] + p
		elif margin > 40:
			final_margin["> 40 " + sign] = final_margin["> 40 " + sign] + p
		elif margin == 0:
			final_margin["-5 < x <= 0 " + sign] = final_margin["-5 < x <= 0 " + sign] + p
		elif margin % 5 != 0:
			division = margin * 1.0 / 5
			up = int(5 * math.ceil(division))
			down = int(5 * math.floor(division))
			final_margin[str(down)+" < x <= "+str(up) + " " + sign] = final_margin[str(down)+" < x <= "+str(up) + " " + sign] + p
		else:
			up = margin
			down = margin - 5
			final_margin[str(down)+" < x <= "+str(up) + " " + sign] = final_margin[str(down)+" < x <= "+str(up) + " " + sign] + p

	list1 = ["team_season_name"] + final_position.keys() + final_margin.keys() + ["Total_+_%","Total_-_%"]
	list2 = row + final_position.values() + final_margin.values() + margin_list
	file_.close()

	# Matplotlib for bar chart

	# N = len(final_position)
	# ind = np.arange(N)
	# width = 0.35
	# plt.bar(range(N),final_position.values(),width,color="r")
	# plt.xlabel(final_position.keys())
	# plt.show()
	return list1, list2

i = 4
result = []
header,row = process_each_file("/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation/15_Spark_Team_Summary/Warriors-Margin Change Summary/part-00000","Warriors","15")
result.append(header)
print len(row)
while (i <= 15):
	os.chdir("/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation/"+str(i)+"_Spark_Team_Summary")
	dirList = os.listdir("./")
	for dirr in dirList:
		if (dirr != ".DS_Store"):
			team = dirr.split("-")[0]
			header,row = process_each_file(dirr+"/part-00000", team, str(i))
			result.append(row)
	i = i + 1
os.chdir("/Users/brandonliang/Desktop/*5. NBA Stats Analytics Research/2016-2017 NBA Simulation")
file_ = open("Team_Play_By_Play_Summary_Data_"+str(season_start)+"-"+str(season_end)+".csv","wb")
writer = csv.writer(file_)
for row in result:
	writer.writerow(row)
file_.close()
	
