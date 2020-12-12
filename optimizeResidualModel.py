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
    xr = np.ones((172, ))
    df['xr'] = pd.Series(xr, index=df.index)
    
    
    print("fitting...")
    
    #X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.2)
    linear_regression = LinearRegression()
    mse = 1
    while mse > 0.01:
        y = df['CompPer']
        x = df[['TopSpeed', 'MaxAccel', 'Height', 'Weight', 'Age', 'xr']]
        linear_regression.fit(x, y)
        y_pred = linear_regression.predict(x)
        for ind in range(len(y_pred)):
            e = y_pred[ind] - y[ind]
            if e > 0:
                curr = df.at[ind, 'xr']
                df.at[ind, 'xr'] = curr + 0.1
            else:
                curr = df.at[ind, 'xr']
                df.at[ind, 'xr'] = curr - 0.1
        #print("Predicted:           Actual:       Diff:")
        #print(list(zip(y_pred, y_test, (y_pred-y_test))), '\n')
        mse = np.sqrt(metrics.mean_squared_error(y, y_pred))
        print(mse)
    print(df['xr'])
    
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


if __name__ == "__main__":
    main()