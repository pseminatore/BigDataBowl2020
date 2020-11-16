import matplotlib.pyplot as plt
import matplotlib.pylab as plb
import dataStore
import sqlite3
from sqlite3 import Error
import numpy as np

def main():
    connection = dataStore.create_data_store()
    cursor = connection.cursor()

    plays = dataStore.get_separation_and_epa_by_play(cursor)
    separations = [play[1] for play in plays]
    play_guids = [play[0] for play in plays]
    yards_gained = []
    index = 0
    for play_guid in play_guids:
        gameId, playId = parse_guid(play_guid)
        yards = dataStore.get_yards_gained_by_play(cursor, gameId, playId)
        if yards:
            yards_gained.append(int(yards[0][0]))
        else:
            del separations[index]
        index += 1
    plt.scatter(separations, yards_gained)
    plt.ylabel("Yards Gained By Offense")
    plt.xlabel("Distance Between Receiver And Defender (Yards)")
    plt.title("Effect of Receiver Separation At Time of Throw On Yards Gained By Offense")
    plt.figtext(0.1, 0.02, "Data: @NextGenStats | Figure: @NSportsline")
    plt.show()

def parse_guid(play_guid):
    guid = str(play_guid)
    gameId = guid[:10]
    playId = guid[10:]
    return int(gameId), int(playId)


if __name__ == "__main__":
    main()