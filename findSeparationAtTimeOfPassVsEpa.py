import dataStore
import sqlite3
from sqlite3 import Error
import findClosestCoveragePlayers
import math
from operator import itemgetter

def main():
    connection = dataStore.create_data_store()
    cursor = connection.cursor()
    dataStore.create_separation_vs_epa_by_play_table(cursor)
    connection.commit()
    plays = dataStore.get_epa_by_play(cursor)
    for play in plays:
        raw_target_receiver = dataStore.get_target_receiver_by_play(cursor, play[0], play[1])
        if not raw_target_receiver == []:
            target_receiver = raw_target_receiver[0][0]
            raw_pass_thrown_frameId = dataStore.get_frameId_and_time_where_pass_attempted(cursor, play[0], play[1])
            if raw_pass_thrown_frameId == 1:
                pass
            else:
                pass_thrown_frameId = int(raw_pass_thrown_frameId[0])
                closest_defender_distance = find_closest_defender(cursor, play[0], play[1], pass_thrown_frameId, target_receiver)
                if closest_defender_distance == -1:
                    pass
                else:
                    play_guid = create_play_guid(play[0], play[1])
                    dataStore.record_separation_vs_epa_by_play(cursor, [play_guid, closest_defender_distance[1], play[2]])
    connection.commit()
    
    
def find_closest_defender(cursor, gameId, playId, frameId, targetNflId):
    target_location = dataStore.get_target_receiver_location(cursor, gameId, playId, frameId, targetNflId)
    if target_location == -1:
        return -1
    defenders = dataStore.get_nflIds_from_play(cursor, gameId, playId, frameId)
    if defenders == []:
        return -1
    defender_distances = []
    for defender in defenders:
        defender_location = dataStore.get_target_defender_location(cursor, gameId, playId, frameId, int(defender[0]))
        defender_distance = calculate_distance(defender_location, target_location)
        defender_distances.append([int(defender[0]), defender_distance])
    closest_defender = sorted(defender_distances, key=itemgetter(1))[0]
    return closest_defender

def calculate_distance(defender_location, target_location):
    distance = math.sqrt(abs((defender_location[0][0] - target_location[0][1]) ** 2 + (defender_location[0][1] - target_location[0][2]) ** 2))
    return distance

def create_play_guid(gameId, playId):
    guid = int(str(gameId) + str(playId))
    return guid
    
if __name__ == "__main__":
    main()