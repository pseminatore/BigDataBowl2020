import dataReader
import dataStore
import sqlite3
from sqlite3 import Error

game = dataReader.readGameData()[0]
tracking = dataReader.readTrackingData()

connection = dataStore.create_data_store()
cursor = connection.cursor()

dataStore.create_avg_separation_table(cursor)
dataStore.create_games_table(cursor)
dataStore.create_tracking_table(cursor)

dataStore.populate_games_table(cursor, game)
dataStore.populate_tracking_table(cursor, tracking) 
connection.commit()