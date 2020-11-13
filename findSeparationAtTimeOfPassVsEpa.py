import dataStore
import sqlite3
from sqlite3 import Error
import findClosestCoveragePlayers


def main():
    connection = dataStore.create_data_store()
    cursor = connection.cursor()
    plays = dataStore.get_epa_by_play(cursor)
    for play in plays:
        target_receiver = dataStore.get_target_receiver_by_play(cursor, play[0], play[1])[0][0]
    
if __name__ == "__main__":
    main()