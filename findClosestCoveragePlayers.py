import dataReader
import dataStore
import sqlite3
from sqlite3 import Error
import os
import math
from operator import itemgetter


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
        player_distances = []
        
        for nflId_tuple in nflIds:
            
            nflId = int(nflId_tuple[0])
            playIds = dataStore.get_playIds_by_player(cursor, gameId, nflId)
            player_avg_distances = []
            
            for playId_tuple in playIds:
                
                playId = int(playId_tuple[0])
                frameIds = dataStore.get_frameIds_by_play(cursor, gameId, nflId, playId)
                player_play_distances = []
                
                for frameId_tuple in frameIds[15:]:
                
                    frameId = int(frameId_tuple[0])
                    locations = dataStore.get_locations_by_frame(cursor, gameId, playId, frameId)
                    def_location = dataStore.get_target_defender_location(cursor, gameId, playId, frameId, nflId)
                    closest_off_player = calculate_min_distance(locations, def_location)
                    player_play_distances.append(closest_off_player)
                    print(closest_off_player)
                
                average_distance = calculate_avg_distance(player_play_distances)
                player_avg_distances.append(average_distance)
                
            average_distance_across_plays = calculate_avg_distance_across_plays(player_avg_distances)
            player_avg_record = [nflId, average_distance_across_plays, len(player_avg_distances)]
            player_distances.append(player_avg_record)
                    
                    
                    
        
        

        
    
def calculate_min_distance(off_locations, def_location):
    distances = []
    for off_location in off_locations:
        distance = [math.sqrt(abs((off_location[0] - def_location[0][0]) + (off_location[1] - def_location[0][1]))), off_location[2]]
        distances.append(distance)
    min_distance = sorted(distances, key=itemgetter(0))[0]
    return min_distance

def calculate_avg_distance(distances):
    sum_distance = 0
    for distance in distances:
        sum_distance += distance[0]
    avg_distance = sum_distance / len(distances)
    return avg_distance

def calculate_avg_distance_across_plays(player_avg_distances):
    sum_distance = 0
    for distance in player_avg_distances:
        sum_distance += distance
    avg_distance = sum_distance / len(player_avg_distances)
    return avg_distance
 
def test_connection():
    data = dataReader.readGameData()[0]
    connection = dataStore.create_data_store()
    dataStore.create_games_table(connection)
    #dataStore.populate_table(connection, data)

if __name__ == "__main__":
    main()