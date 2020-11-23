import dataReader
import dataStore
import sqlite3
from sqlite3 import Error
import os
import math
import matplotlib.pyplot as plt
from operator import itemgetter


def main(): 
    connection = dataStore.create_data_store()
    cursor = connection.cursor()
    

    gameIds = dataStore.get_all_gameIds(cursor)
    
    for gameId_tuple in gameIds:
        
        gameId = int(gameId_tuple[0])
        nflIds = dataStore.get_nflIds_from_game(cursor, gameId)
        
        ## Get nflIds for all coverage players
        for nflId_tuple in nflIds:
            
            nflId = int(nflId_tuple[0])
            playIds = dataStore.get_playIds_by_player(cursor, gameId, nflId)
            plays_defended = []
            ## TODO - attempt to filter out zone coverage
            ## Get players average distance from coverage target on each play
            for playId_tuple in playIds:
                
                playId = int(playId_tuple[0])
                frameIds = dataStore.get_frameIds_by_play(cursor, gameId, nflId, playId, stop_frame_id)
                
                ## Find target receiver on each play
                target_receiver = 
                
                ## Find frame where pass thrown
                frame_id_pass_thrown = 
                
                ## Find closest defender when pass thrown
                closest_defender = 
                
                ## Calculate defenders Zone of Influence
                defender_zoi = 
                
                ## Is receiver within ZoI when pass thrown?
                defender_influence = 
                
                ## If so, is the pass defended?
                if defender_influence:
                    pass_defended = 
                    plays_defended.append(pass_defended)
            
            dataStore.record_player_effeciency_def_zoi(nflId, len(playIds), '''number of plays within zoi''', '''(%) defended given within zoi''')
                    
                
    connection.commit()


def extract_names_from_nflIds(cursor, nflIds):
    names = []
    for nflId in nflIds:
        name = dataStore.get_name_by_nflId(cursor, nflId)
        names.append(name)
    return names
    
def calculate_min_distance(off_locations, def_location):
    distances = []
    for off_location in off_locations:
        distance = [math.sqrt(abs((off_location[0] - def_location[0][0]) + (off_location[1] - def_location[0][1]))), off_location[2]]
        distances.append(distance)
    min_distance = sorted(distances, key=itemgetter(0))[0]
    return min_distance
 

if __name__ == "__main__":
    main()