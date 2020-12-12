import dataStore
import sqlite3
from sqlite3 import Error
import numpy as np
from operator import itemgetter
import plotly.express as px
import pandas as pd

def main():
    connection = dataStore.create_data_store()
    cursor = connection.cursor()
    names_arr, eff_arr, speed_arr = grab_best_players(cursor, 50, 100)
    df = pd.DataFrame(list(zip(eff_arr, speed_arr, names_arr)), columns=['CompletionPercentage', 'TopSpeed', 'PlayerName'])
    fig = px.scatter(df, x='CompletionPercentage', y='TopSpeed', hover_data=['PlayerName'])
    fig.show()
    fig.write_html("passDefendingVsTopSpeed.html")
    
def grab_best_players(cursor, min_attempts, num_players):
    players = dataStore.get_zoi_players(cursor, min_attempts)
    raw_zoi_arr = create_zoi_array(cursor, players)
    sorted_best_players = sorted(raw_zoi_arr, key=itemgetter(1))[:num_players]
    speed_arr = get_top_speeds(cursor, sorted_best_players)
    sorted_with_names = get_player_names(cursor, sorted_best_players)
    names_arr = [name[0] for name in sorted_with_names]
    eff_arr = [name[1] for name in sorted_with_names]
    return names_arr, eff_arr, speed_arr

def get_top_speeds(cursor, names_arr):
    speed_arr = []
    for name in names_arr:
        top_speed = dataStore.get_top_speed_by_nflId(cursor, name[0])
        speed_arr.append(top_speed[0][0])
    return speed_arr


def create_zoi_array(c, players):
    zoi_eff_arr = []
    for player in players:
        sum_comp = 0
        completions = dataStore.get_zoi_plays_by_player(c, player[0])
        for entry in completions:
            sum_comp += entry[0]
        eff_data = [player[0], sum_comp/len(completions)]
        zoi_eff_arr.append(eff_data)
    return zoi_eff_arr    

def get_player_names(c, sorted_players):
    names_efficiency = []
    for player in sorted_players:
        name = dataStore.get_name_by_nfl(c, player[0])[0][0]
        name_eff_data = [name, player[1]]
        names_efficiency.append(name_eff_data)
    return names_efficiency

if __name__ == "__main__":
    main()