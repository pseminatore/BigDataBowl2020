import dataReader
import dataStore
import sqlite3
from sqlite3 import Error
import os
import math
from operator import itemgetter


def main(frames_project_forward=5): 
    connection = dataStore.create_data_store()
    cursor = connection.cursor()
    counter = 0

    gameIds = dataStore.get_all_gameIds(cursor)
    
    for gameId_tuple in gameIds:
        
        gameId = int(gameId_tuple[0])
        playIds = dataStore.get_playIds_from_game(cursor, gameId)
            
        for playId_tuple in playIds:
            
            playId = int(playId_tuple[0])
            
            ## Find target receiver on each play
            raw_targ = dataStore.get_target_receiver_by_play(cursor, gameId, playId)
            
            if raw_targ:
                target_receiver = raw_targ[0][0]
                
                ## Find frame where pass thrown
                was_thrown = dataStore.get_frameId_and_time_where_pass_attempted(cursor, gameId, playId)
                if isinstance(was_thrown, tuple):
                    
                    frame_id_pass_thrown = int(was_thrown[0])
                    
                    defenders = dataStore.get_nflIds_from_play(cursor, gameId, playId, frame_id_pass_thrown)
                    
                    for defender in defenders:
                        # Look forward to ensure ZoI is dynamic
                        rad_influence, proj_defender_location = project_zoi_forward(cursor, defender, target_receiver, gameId, playId, frame_id_pass_thrown, frames_project_forward)
                        if rad_influence == -1 and proj_defender_location == -1:
                            pass
                        else:
                            raw_rec_loc = dataStore.get_target_receiver_location(cursor, gameId, playId, frame_id_pass_thrown + frames_project_forward, target_receiver)
                        
                            if isinstance(raw_rec_loc, list):
                                proj_receiver_location = raw_rec_loc[0]
                                
                                # Can the defender influence the play?
                                in_zoi = int(is_receiver_in_zoi(rad_influence, proj_defender_location, proj_receiver_location))
                                
                                # Was the pass completed?
                                query_pass_completed = dataStore.get_is_pass_completed(cursor, gameId, playId)
                                
                                if query_pass_completed:
                                    
                                    pass_completed = query_pass_completed[0][1]
                                    data = [defender[0], playId, gameId, in_zoi, pass_completed]
                                    dataStore.record_zoi_completion_by_play(cursor, data)
                                    counter+=1
                                    #print (counter)                   
            
    connection.commit()

def project_zoi_forward(cursor, defender, target_receiver, gameId, playId, frameId, frames_project_forward):
    try:
        proj_frameId = frameId + frames_project_forward
        proj_defender_location = dataStore.get_target_defender_location(cursor, gameId, playId, proj_frameId, defender[0])[0]
        proj_ball_location = dataStore.get_ball_location(cursor, gameId, playId, proj_frameId)[0]
        proj_zoi_rad = calculate_zoi(proj_defender_location, proj_ball_location)
        return proj_zoi_rad, proj_defender_location
    except Exception:
        return -1, -1
    
def calculate_zoi(proj_defender_location, proj_ball_location):
    dist_from_ball = calculate_distance(proj_defender_location, proj_ball_location)
    if dist_from_ball > 18:
        return 10
    else:
        return 4 + (6/(18 ** 2)) * (dist_from_ball ** 2)

def calculate_distance(proj_defender_location, proj_ball_location):
    distance = math.sqrt(abs(((proj_defender_location[0] - proj_ball_location[0]) ** 2) + ((proj_defender_location[1] - proj_ball_location[1]) ** 2)))
    return distance

def is_receiver_in_zoi(rad_influence, proj_defender_location, proj_receiver_location):
    distance = math.sqrt(abs(((proj_defender_location[0] - proj_receiver_location[1]) ** 2) + ((proj_defender_location[1] - proj_receiver_location[2]) ** 2)))
    return distance <= rad_influence
         
def extract_names_from_nflIds(cursor, nflIds):
    names = []
    for nflId in nflIds:
        name = dataStore.get_name_by_nflId(cursor, nflId)
        names.append(name)
    return names
    

if __name__ == "__main__":
    main()