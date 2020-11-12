import dataReader
import dataStore
import sqlite3
from sqlite3 import Error

game = dataReader.readGameData()[0]
tracking = dataReader.readTrackingData()
plays = dataReader.readPlayData()[0]

connection = dataStore.create_data_store()
cursor = connection.cursor()

dataStore.create_avg_separation_table(cursor)
dataStore.create_games_table(cursor)
dataStore.create_plays_table(cursor)
dataStore.create_tracking_table(cursor)
dataStore.create_avg_separation_by_player_table(cursor)

dataStore.populate_plays_table(cursor, plays)
dataStore.populate_games_table(cursor, game)
dataStore.populate_tracking_table(cursor, tracking) 
connection.commit()