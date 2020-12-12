import dataStore
import sqlite3
from sqlite3 import Error
import numpy as np
from operator import itemgetter
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn import metrics
import plotly.express as px


def main():
    connection = dataStore.create_data_store()
    cursor = connection.cursor()
    
    nflId_arr, eff_arr = grab_best_cov_players(cursor, 30)
    #speed_arr = get_top_speeds(cursor, nflId_arr)
    #print("computed speeds")
    #accel_arr = get_max_accel(cursor, nflId_arr)
    #print("computed acc")
    #height_arr, weight_arr, age_arr = get_player_physical_info(cursor, nflId_arr)
    #data_list = list(zip(nflId_arr, eff_arr, speed_arr, accel_arr, height_arr, weight_arr, age_arr))
    #dataStore.record_model_input_data(cursor, data_list)
    #connection.commit()
    data_list = dataStore.get_modelinfo_by_nflId(cursor, nflId_arr)
    df = pd.DataFrame(data_list, columns=['NflId', 'CompPer', 'TopSpeed', 'MaxAccel', 'Height', 'Weight', 'Age'])
    y = df['CompPer']
    x = df[['TopSpeed', 'MaxAccel', 'Height', 'Weight', 'Age']]
    
    print("fitting...")
    
    X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.2)
    linear_regression = LinearRegression()
    linear_regression.fit(x, y)
    
    y_pred = linear_regression.predict(x)
    #print("Predicted:           Actual:       Diff:")
    #print(list(zip(y_pred, y_test, (y_pred-y_test))), '\n')
    print(np.sqrt(metrics.mean_squared_error(y, y_pred)))
    fig = px.scatter(df, )
    
def grab_best_cov_players(cursor, min_attempts):
    players = dataStore.get_zoi_players(cursor, min_attempts)
    raw_zoi_arr = create_zoi_array(cursor, players)
    sorted_best_players = sorted(raw_zoi_arr, key=itemgetter(1))
    nflId_arr = [name[0] for name in sorted_best_players]
    eff_arr = [name[1] for name in sorted_best_players]
    return nflId_arr, eff_arr

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

def get_top_speeds(cursor, nflId_arr):
    speed_arr = []
    for player in nflId_arr:
        top_speed = dataStore.get_top_speed_by_nflId(cursor, player)
        speed_arr.append(top_speed[0][0])
    return speed_arr

def get_max_accel(cursor, nflId_arr):
    acc_arr = []
    for player in nflId_arr:
        acc_data = dataStore.get_max_accel_by_nflId(cursor, player)
        acc_arr.append(acc_data[0][0])
    return acc_arr

def get_player_physical_info(cursor, nflId_arr):
    height_arr = []
    weight_arr = []
    age_arr = []
    for player in nflId_arr:
        
        data_arr = dataStore.get_playerinfo_by_nflId(cursor, player)
        
        height = clean_height(data_arr[0][0])
        weight = data_arr[0][1]
        age = get_age(data_arr[0][2])
        
        height_arr.append(height)
        weight_arr.append(weight)
        age_arr.append(age)
        
    return height_arr, weight_arr, age_arr

def clean_height(height):
    if '-' in height:
        ft = int(height[0])
        inc = int(height[2:])
        height_in = (ft * 12) + inc
    else:
        height_in = int(height)
    return height_in

def get_age(_date):
    if '-' in _date:
        year = _date[:4]
        month = _date[5:7]
        day = _date[8:]
    else:
        month = _date[:2]
        day = _date[3:5]
        year = _date[6:]
        
    birthdate = date(int(year), int(month), int(day))
    seasondate = date(2018, 9, 1)
    age = relativedelta(seasondate, birthdate)
    age = age.years
    return age

if __name__ == "__main__":
    main()