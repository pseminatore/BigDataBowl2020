import dataReader
import dataStore
import sqlite3
from sqlite3 import Error
import os


def main():
    game = dataReader.readGameData()[0]
    tracking = dataReader.readWeek1TrackingData()
    
    connection = dataStore.create_data_store()
    cursor = connection.cursor()
    
    ## Set up data tables - only needed to run once
    """ dataStore.create_games_table(cursor)
    dataStore.create_tracking_table(cursor)
    
    dataStore.populate_games_table(cursor, game)
    dataStore.populate_tracking_table(cursor, tracking)
    connection.commit() """
    
    gameIds = dataStore.get_all_gameIds(cursor)
    
    for gameId_tuple in gameIds:
        
        gameId = int(gameId_tuple[0])
        nflIds = dataStore.get_nflIds_from_game(cursor, gameId)
        
        for nflId_tuple in nflIds:
            
            nflId = int(nflId_tuple[0])
            playIds = dataStore.get_playIds_by_player(cursor, gameId, nflId)
            
            for playId_tuple in playIds:
                
                playId = int(playId_tuple[0])
                frameIds = dataStore.get_frameIds_by_play(cursor, gameId, nflId, playId)
                
                for frameId_tuple in frameIds[10:]:
                
                    frameId = int(frameId_tuple[0])
                    locations = dataStore.get_locations_by_frame(cursor, gameId, playId, frameId)
        
        

        
    

    
def test_connection():
    data = dataReader.readGameData()[0]
    connection = dataStore.create_data_store()
    dataStore.create_games_table(connection)
    #dataStore.populate_table(connection, data)

if __name__ == "__main__":
    main()