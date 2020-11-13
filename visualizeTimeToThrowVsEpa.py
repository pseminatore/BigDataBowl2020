import matplotlib.pyplot as plt
import matplotlib.pylab as plb
import dataStore
import sqlite3
from sqlite3 import Error
import numpy as np

connection = dataStore.create_data_store()
cursor = connection.cursor()

plays = dataStore.get_time_to_throw_and_epa_by_play(cursor)
epas = [play[1] for play in plays]
times = [play[0] for play in plays]
plt.scatter(times, epas)
plt.ylabel("Expected Points Added (Points/Play)")
plt.xlabel("Time Elapsed From Snap To Throw (sec)")
plt.title("Effect of Time To Throw On Offensive Success")
plt.figtext(0.1, 0.02, "Data: @NextGenStats | Figure: @NSportsline")
plt.show()