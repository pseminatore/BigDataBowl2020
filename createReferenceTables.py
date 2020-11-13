import dataReader
import dataStore
import sqlite3
from sqlite3 import Error

game = dataReader.readGameData()[0]
tracking = dataReader.readTrackingData()
plays = dataReader.readPlayData()[0]
targets = dataReader.readTargetReceiverData()[0]

connection = dataStore.create_data_store()
cursor = connection.cursor()

dataStore.create_avg_separation_table(cursor)
dataStore.create_games_table(cursor)
dataStore.create_plays_table(cursor)
dataStore.create_tracking_table(cursor)
dataStore.create_avg_separation_by_player_table(cursor)
dataStore.create_time_to_throw_vs_epa_table(cursor)
dataStore.create_target_receiver_table(cursor)


dataStore.populate_targets_table(cursor, targets)
#dataStore.populate_plays_table(cursor, plays)
#dataStore.populate_games_table(cursor, game)
#dataStore.populate_tracking_table(cursor, tracking) 
connection.commit()