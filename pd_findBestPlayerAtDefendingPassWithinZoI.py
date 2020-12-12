import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import glob
import os
import math


def main(frames_project_forward=5):
    # Input data files are available in the read-only "../input/" directory
    # For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory

    games_df = pd.read_csv('Data/games.csv')
    plays_df = pd.read_csv('Data/plays.csv')
    players_df = pd.read_csv('Data/players.csv')
    targeted_rec_df = pd.read_csv('Data/targetedReceiver.csv').dropna()

    tracking_files = glob.glob("Data/Tracking/week*.csv")

    li = []

    for filename in tracking_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    tracking_df = pd.concat(li, axis=0, ignore_index=True)

    resulting_df = pd.DataFrame(columns=['nflId', 'gameId', 'playId', 'in_zoi', 'pass_completed'])
    
    gameIds = games_df.gameId.unique()
    
    for _gameId in gameIds:
        
        playIds = tracking_df[tracking_df.gameId == _gameId].playId.unique()
        
        for _playId in playIds:
            try:
                target_receiver = (targeted_rec_df[(targeted_rec_df.gameId == _gameId) & (targeted_rec_df.playId == _playId)].targetNflId).values[0]
                frame_pass_thrown = (tracking_df[(tracking_df.gameId == _gameId) & (tracking_df.playId == _playId) & ((tracking_df.event == 'pass_lateral') | (tracking_df.event == 'pass_forward'))].frameId).values[0]
                defender_position_list = ["SS", "FS", "MLB", "LB", "CB"]
                defenders = (tracking_df[(tracking_df.gameId == _gameId) & (tracking_df.playId == _playId) & (tracking_df.frameId == frame_pass_thrown) & tracking_df.position.isin(defender_position_list)].nflId).values
            
                for defender in defenders:
                    
                    rad_influence, proj_defender_location = project_zoi_forward(defender, target_receiver, _gameId, _playId, frame_pass_thrown, frames_project_forward, tracking_df)
                
                    if rad_influence == -1 and proj_defender_location == -1:
                        pass
                    
                    else:
                        target_receiver_loc = (tracking_df.loc[(tracking_df.gameId == _gameId) & (tracking_df.playId == _playId) & (tracking_df.frameId == (frame_pass_thrown + frames_project_forward)) & (tracking_df.nflId == target_receiver), ["x", "y", "nflId"]]).values[0]
                        
                        # Can the defender influence the play?
                        in_zoi = int(is_receiver_in_zoi(rad_influence, proj_defender_location, target_receiver_loc))
                        
                        # Was the pass completed?
                        completed = -1
                        pass_c_event_list = ["pass_outcome_caught", "pass_outcome_touchdown"]
                        pass_i_event_list = ["pass_outcome_incomplete", "pass_outcome_interception"]
                        
                        pass_event = (tracking_df.loc[(tracking_df.gameId == _gameId) & (tracking_df.playId == _playId) & ((tracking_df.event.isin(pass_c_event_list)) | (tracking_df.event.isin(pass_i_event_list))), ["event"]]).values[0][0]
                        if pass_event in pass_c_event_list:
                            completed = 1
                            resulting_df.append({'nflId' : defender, 'gameId' : _gameId, 'playId' : _playId, 'in_zoi' : in_zoi, 'pass_completed' : completed}, ignore_index = True)
                        elif pass_event in pass_i_event_list:
                            completed = 0
                            resulting_df.append({'nflId' : defender, 'gameId' : _gameId, 'playId' : _playId, 'in_zoi' : in_zoi, 'pass_completed' : completed}, ignore_index = True)
                        else:
                            pass
            except Exception:
                pass
    resulting_df.head()

def project_zoi_forward(defender, target_receiver, gameId, playId, frameId, frames_project_forward, tracking_df):
    try:
        proj_frameId = frameId + frames_project_forward
        proj_defender_location = (tracking_df.loc[(tracking_df.gameId == gameId) & (tracking_df.playId == playId) & (tracking_df.frameId == proj_frameId) & (tracking_df.nflId == defender), ["x", "y", "nflId"]]).values[0]
        proj_ball_location = (tracking_df.loc[(tracking_df.gameId == gameId) & (tracking_df.playId == playId) & (tracking_df.frameId == proj_frameId) & (tracking_df.displayName == "Football"), ["x", "y"]]).values[0]
        proj_zoi_rad = calculate_zoi(proj_defender_location, proj_ball_location)
        return proj_zoi_rad, proj_defender_location
    except Exception as e:
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
    distance = math.sqrt(abs(((proj_defender_location[0] - proj_receiver_location[0]) ** 2) + ((proj_defender_location[1] - proj_receiver_location[1]) ** 2)))
    return distance <= rad_influence

if __name__ == "__main__":
    main()