import dataStore
import sqlite3
from sqlite3 import Error
import datetime

def main():
    connection = dataStore.create_data_store()
    cursor = connection.cursor()
    plays = dataStore.get_epa_by_play(cursor)
    
    ## All plays across all games
    for play in plays:
        ball_snap = dataStore.get_frameId_where_ball_snapped(cursor, play[0], play[1])
        pass_thrown = dataStore.get_frameId_and_time_where_pass_attempted(cursor, play[0], play[1])
        if pass_thrown == 1:
            pass
        else:
            time_of_snap = parse_time(ball_snap[0])
            time_of_pass = parse_time(pass_thrown)
            time_to_pass = (time_of_pass - time_of_snap).total_seconds()
            play_guid = create_play_guid(play[0], play[1])
            data_frame = [play_guid, play[2], time_to_pass]
            dataStore.record_time_to_throw_vs_epa_by_play(cursor, data_frame)
    connection.commit()    
        
def parse_time(event):
        datetime_of_event_str = event[1][:-1]
        time_of_event_str = datetime_of_event_str[datetime_of_event_str.index('T') + 1:]
        time_of_event = datetime.datetime.strptime(time_of_event_str, '%H:%M:%S.%f')   
        return time_of_event

def create_play_guid(gameId, playId):
    guid = int(str(gameId) + str(playId))
    return guid
        
if __name__ == "__main__":
    main()    