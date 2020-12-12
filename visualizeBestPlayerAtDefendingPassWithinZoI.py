import dataStore
import sqlite3
from sqlite3 import Error
import numpy as np
from operator import itemgetter
import plotly.express as px


def main():
    connection = dataStore.create_data_store()
    cursor = connection.cursor()
    
    names_arr, eff_arr = grab_best_players(cursor, 10, 10)
    fig = px.bar( x=names_arr, y=eff_arr, 
                 labels={'x': 'Defender', 'y': 'Completion Percentage Allowed'})
    
    names_arr, eff_arr = grab_best_players(cursor, 20, 10)
    fig2 = px.bar( x=names_arr, y=eff_arr, labels={'x': 'Defender', 'y': 'Completion Percentage Allowed'})
    fig.add_trace(fig2.data[0])
    
    names_arr, eff_arr = grab_best_players(cursor, 30, 10)
    fig3 = px.bar( x=names_arr, y=eff_arr, labels={'x': 'Defender', 'y': 'Completion Percentage Allowed'})
    fig.add_trace(fig3.data[0])
    
    names_arr, eff_arr = grab_best_players(cursor, 50, 10)
    fig4 = px.bar( x=names_arr, y=eff_arr, labels={'x': 'Defender', 'y': 'Completion Percentage Allowed'})
    fig.add_trace(fig4.data[0])
    
    names_arr, eff_arr = grab_best_players(cursor, 75, 10)
    fig5 = px.bar( x=names_arr, y=eff_arr, labels={'x': 'Defender', 'y': 'Completion Percentage Allowed'})
    fig.add_trace(fig5.data[0])
    
    
    fig.update_layout(
        yaxis=dict(tickformat=".2%"),
        title={
        'text': "Lowest Completion % Allowed When Targeted Receiver Within Defender Radius of Influence",
        'y':0.99,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
        annotations=[
        dict(text="Minimum Plays within RoI:", showarrow=False,
                             x=4.8, y=1.05, yref="paper", align="right")
        ],
        updatemenus=[
        dict(
            direction="down",
            pad={"r": 10, "t": 10},
            xanchor="left",
            yanchor="top",
            x=0.6,
            y=1.08,
            showactive=True,
            buttons=list(
                [
                    dict(
                        label="10",
                        method="update",
                        args=[
                            {"visible": [True, False, False, False, False]},
                            ],
                    ),
                    dict(
                        label="20",
                        method="update",
                        args=[
                            {"visible": [False, True, False, False, False]},
                            ],
                        ),
                    dict(
                        label="30",
                        method="update",
                        args=[
                            {"visible": [False, False, True, False, False]},
                            ],
                        ),
                    dict(
                        label="50",
                        method="update",
                        args=[
                            {"visible": [False, False, False, True, False]},
                            ],
                        ),
                    dict(
                        label="75",
                        method="update",
                        args=[
                            {"visible": [False, False, False, False, True]},
                            ],
                        ),
                    ]
                ),
            )
        ]
    )
    fig.show()
    fig.write_html("bestPlayerAtDefendingPassWithinZoI.html")
    


def grab_best_players(cursor, min_attempts, num_players):
    players = dataStore.get_zoi_players(cursor, min_attempts)
    raw_zoi_arr = create_zoi_array(cursor, players)
    sorted_best_players = sorted(raw_zoi_arr, key=itemgetter(1))[:num_players]
    sorted_with_names = get_player_names(cursor, sorted_best_players)
    names_arr = [name[0] for name in sorted_with_names]
    eff_arr = [name[1] for name in sorted_with_names]
    return names_arr, eff_arr
    
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