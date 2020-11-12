import matplotlib.pyplot as plt
import dataStore
import sqlite3
from sqlite3 import Error

connection = dataStore.create_data_store()
cursor = connection.cursor()

plays = dataStore.get_num_pass_rushers_and_epa_by_play(cursor)
numPassRushers = [int(play[0]) for play in plays]
epas = [float(play[1]) for play in plays]
plt.scatter(numPassRushers, epas)
plt.ylabel("Expected Points Added (Points/Play)")
plt.xlabel("Number of Pass Rushers")
plt.title("Effect of Number of Pass Rushers On Offensive Success")
plt.show()