import dataReader
import dataStore
import sqlite3
from sqlite3 import Error
import matplotlib.pyplot as plt
from operator import itemgetter
import findClosestCoveragePlayers


connection = dataStore.create_data_store()
cursor = connection.cursor()

closest_coverage_players = findClosestCoveragePlayers.get_top_ten_players(cursor)
closest_coverage_distance = findClosestCoveragePlayers.extract_best_averages(closest_coverage_players)
findClosestCoveragePlayers.populate_separation_by_player(cursor)
connection.commit()

plt.bar(range(len(closest_coverage_players)), closest_coverage_distance, width=0.6)
plt.ylabel("Average Separation From Closest Receiver (yards)")
plt.title("Which Players Minimized Separation From Receivers In 2019?")
plt.rcParams.update({'font.size': 6})
x_pos = [i for i in range(len(closest_coverage_players))]
plt.xticks(x_pos, findClosestCoveragePlayers.extract_names_from_nflIds(cursor, findClosestCoveragePlayers.extract_nflIds_from_best(closest_coverage_players)), rotation=15)

plt.figtext(0.1, 0.02, "Min 200 Coverage Snaps | Data: @NextGenStats | Figure: @NSportsline")

for x,y in zip(x_pos, closest_coverage_distance):
    
    label = "{:.2f}".format(y)

    plt.annotate(label, # this is the text
                (x,y), # this is the point to label
                textcoords="offset points", # how to position the text
                xytext=(0,10), # distance from text to points (x,y)
                ha='center') # horizontal alignment can be left, right or center
plt.show()