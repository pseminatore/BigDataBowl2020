import dataReader
import dataStore
import sqlite3
from sqlite3 import Error
import findClosestCoveragePlayers


def main():
    connection = dataStore.create_data_store()
    cursor = connection.cursor()
    dataStore.create_avg_separation_and_epa_by_play_table(cursor)
    plays = dataStore.get_epa_by_play(cursor)
    
    for play in plays:
        frame = dataStore.get_first_frame_of_play(cursor, play[0], play[1])
        if frame:
            first_frameId = frame[0][0]
        else:
            pass
        def_nflIds = dataStore.get_nflIds_from_play(cursor, play[0], play[1], first_frameId)
        stop_frame = dataStore.get_frameId_where_pass_arrives(cursor, play[0], play[1])
        play_avg_separations = []
        for def_nflId in def_nflIds:
            
            frameIds = dataStore.get_frameIds_by_play(cursor, play[0], def_nflId[0], play[1], stop_frame)
            player_frame_distances = []
            
            for frameId_tuple in frameIds[15:]:
                
                frameId = int(frameId_tuple[0])
                locations = dataStore.get_locations_by_frame(cursor, play[0], play[1], frameId)
                def_location = dataStore.get_target_defender_location(cursor, play[0], play[1], frameId, def_nflId[0])
                
                if locations and def_location:
                    closest_off_player = findClosestCoveragePlayers.calculate_min_distance(locations, def_location)
                    player_frame_distances.append(closest_off_player)
                    
                else:
                    pass
                
            if player_frame_distances:
                frame_avg_distance = calculate_avg_frame_distance(player_frame_distances)
                play_avg_separations.append([def_nflId[0], frame_avg_distance])
                
            else:
                pass
            
        if play_avg_separations:
            play_avg_separation = calculate_avg_play_distance(play_avg_separations)
            play_guid = create_play_guid(play[0], play[1])
            data = [play_guid, play[2], play_avg_separation]
            dataStore.record_avg_separation_and_epa_by_play(cursor, data)
        else:
            pass
    connection.commit()
        
def calculate_avg_frame_distance(player_frame_distances):
    total = 0
    for frame in player_frame_distances:
        total += frame[0]
    return total / len(player_frame_distances)

def calculate_avg_play_distance(play_avg_separations):
    total = 0
    for play in play_avg_separations:
        total += play[1]
    return total / len(play_avg_separations)

def create_play_guid(gameId, playId):
    guid = int(str(gameId) + str(playId))
    return guid
          
if __name__ == "__main__":
    main()