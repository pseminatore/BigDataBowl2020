import dataStore
import sqlite3
from sqlite3 import Error

connection = dataStore.create_data_store()
cursor = connection.cursor()
#dataStore.drop_games_table(cursor)
#dataStore.drop_tracking_table(cursor)
#dataStore.drop_separation_table(cursor)
#dataStore.drop_separation_by_player_table(cursor)
#dataStore.drop_plays_table(cursor)
#dataStore.drop_targets_table(cursor)
dataStore.drop_zoi_completion_by_play_table(cursor)
connection.commit()