import matplotlib.pyplot as plt
import dataStore
import sqlite3
from sqlite3 import Error

connection = dataStore.create_data_store()
cursor = connection.cursor()

plays = dataStore.get_epa_and_avg_separation_by_play(cursor)
epas = [play[1] for play in plays]
separations = [play[2] for play in plays]
plt.scatter(separations, epas)
plt.ylabel("Expected Points Added (Points/Play)")
plt.xlabel("Team's Average Separation From Receivers (Yards per Receiver/Play)")
plt.title("Effect of Close Coverage On Offensive Success")
plt.show()