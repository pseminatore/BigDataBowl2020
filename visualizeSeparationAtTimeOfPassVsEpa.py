import matplotlib.pyplot as plt
import matplotlib.pylab as plb
import dataStore
import sqlite3
from sqlite3 import Error
import numpy as np

connection = dataStore.create_data_store()
cursor = connection.cursor()

plays = dataStore.get_separation_and_epa_by_play(cursor)
epas = [play[2] for play in plays]
separations = [play[1] for play in plays]
plt.scatter(separations, epas)
plt.ylabel("Expected Points Added (Points/Play)")
plt.xlabel("Distance Between Receiver And Defender (Yards)")
plt.title("Effect of Receiver Separation At Time of Throw On Offensive Success")
plt.figtext(0.1, 0.02, "Data: @NextGenStats | Figure: @NSportsline")
plt.show()