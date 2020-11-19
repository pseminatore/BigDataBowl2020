import matplotlib.pyplot as plt
import matplotlib.pylab as plb
import dataStore
import sqlite3
from sqlite3 import Error
import numpy as np

connection = dataStore.create_data_store()
cursor = connection.cursor()

plays = dataStore.get_separation_and_time_to_throw_by_play(cursor)
times = [play[0] for play in plays]
separations = [play[1] for play in plays]
plt.scatter(times, separations)
plt.ylabel("Separation at Time of Throw (Yards)")
plt.xlabel("Time to Throw (Sec)")
plt.title("Time to Throw vs Separation At Time Of Throw")
plt.figtext(0.1, 0.02, "Data: @NextGenStats | Figure: @NSportsline")
plt.show()