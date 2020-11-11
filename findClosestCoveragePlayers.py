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
    data_frame = []
    
    for gameId_tuple in gameIds:
        
        gameId = int(gameId_tuple[0])
        nflIds = dataStore.get_nflIds_from_game(cursor, gameId)
        player_distances = []
        
        ## Get nflIds for all coverage players
        for nflId_tuple in nflIds:
            
            nflId = int(nflId_tuple[0])
            playIds = dataStore.get_playIds_by_player(cursor, gameId, nflId)
            player_avg_distances = []
            
            ## TODO - filter out running plays
            ## TODO - attempt to filter out zone coverage
            ## Get players average distance from coverage target on each play
            for playId_tuple in playIds:
                
                playId = int(playId_tuple[0])
                stop_frame_id = dataStore.get_frameId_where_pass_arrives(cursor, gameId, playId)
                frameIds = dataStore.get_frameIds_by_play(cursor, gameId, nflId, playId, stop_frame_id)
                player_play_distances = []
                
                for frameId_tuple in frameIds[15:]:
                
                    frameId = int(frameId_tuple[0])
                    locations = dataStore.get_locations_by_frame(cursor, gameId, playId, frameId)
                    def_location = dataStore.get_target_defender_location(cursor, gameId, playId, frameId, nflId)
                    closest_off_player = calculate_min_distance(locations, def_location)
                    player_play_distances.append(closest_off_player)
                    #print(closest_off_player)
                if player_play_distances:
                    average_distance = calculate_avg_distance(player_play_distances)
                    player_avg_distances.append(average_distance)
                else:
                    pass
                
            average_distance_across_plays = calculate_avg_distance_across_plays(player_avg_distances)
            player_avg_record = [nflId, average_distance_across_plays, len(player_avg_distances)]
            player_distances.append(player_avg_record)
            dataStore.record_avg_separation_table(cursor, gameId, player_avg_record)
        data_frame.append([gameId, player_distances])
    connection.commit()
    
    
    ## TODO - some sort of visualization    
    closest_coverage_players = get_top_ten_players(cursor)
    closest_coverage_distance = extract_best_averages(closest_coverage_players)
    
    plt.bar(range(len(closest_coverage_players)), closest_coverage_distance, width=0.6)
    plt.ylabel("Average Separation From Closest Receiver (yards)")
    plt.title("Which Players Minimized Separation By Receivers In 2019?")
    plt.rcParams.update({'font.size': 6})
    x_pos = [i for i in range(len(closest_coverage_players))]
    plt.xticks(x_pos, extract_names_from_nflIds(cursor, extract_nflIds_from_best(closest_coverage_players)), rotation=15)
    
    plt.figtext(0.1, 0.02, "Min 30 Coverage Snaps | Data: @NextGenStats | Figure: @NSportsline")

    for x,y in zip(x_pos, closest_coverage_distance):
        
        label = "{:.2f}".format(y)

        plt.annotate(label, # this is the text
                 (x,y), # this is the point to label
                 textcoords="offset points", # how to position the text
                 xytext=(0,10), # distance from text to points (x,y)
                 ha='center') # horizontal alignment can be left, right or center
    plt.show()


def set_minimum_number_plays(player):
    return player[2] > 30

def extract_best_averages(best_players):
    avs = [player[1] for player in best_players]
    return avs

def extract_nflIds_from_best(best_players):
    nflIds = [player[0] for player in best_players]
    return nflIds
       
def get_top_ten_players(cursor):
    players = dataStore.get_nflIds_from_separation_table(cursor)
    players_list = []
    for player in players:
        nflId = int(player[0])
        separations = 0
        plays = 0
        player_record = dataStore.get_separation_for_player_in_separation_table(cursor, nflId)
        if len(player_record) > 0:
            for record in player_record:
                separations += record[0]
                plays += record[1]
            avg_separation = separations / len(player_record)
            players_list.append([player, avg_separation, plays])
        else:
            pass
    ys = filter(set_minimum_number_plays, players_list)
    filtered_players_list = list(ys)
    filtered_players_list.sort(key=itemgetter(1))        
    return filtered_players_list[:10]
        
def extract_nflIds_from_dataframe(data_frame):
    xs = []
    for game in data_frame:
        for player in game[1]:
            xs.append(player[0])
    return xs

def extract_avg_distances_from_dataframe(data_frame):
    ys = []
    for game in data_frame: 
        for player in game[1]:
            ys.append(player[1])
    return ys

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