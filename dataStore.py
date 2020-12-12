import sqlite3
from sqlite3 import Error
import os

## -----------------------------------------------------------CREATE DATABASE AND CONNECTION-----------------------------------------------------------------##
def create_connection(path):
    connection = None
    
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
        
    except Error as e:
        print(f"The error '{e}' occurred")
        
    return connection  

def create_data_store():
    connection = create_connection(os.getcwd() + "\\datastore.sqlite")
    return connection

##-------------------------------------------------------------------CREATE TABLES---------------------------------------------------------------------------##
def create_tracking_table(c):
    query = '''CREATE TABLE IF NOT EXISTS tracking (
        time text NOT NULL,
        x real NOT NULL,
        y real NOT NULL,
        s real,
        a real,
        dis real,
        o real,
        dir real,
        event text,
        nflId real NOT NULL,
        displayName text,
        jerseyNumber real,
        position text,
        team text,
        frameID real NOT NULL,
        gameID real NOT NULL,
        playID real NOT NULL,
        playDirection text,
        route text,
        PRIMARY KEY (gameID, playID, frameID, nflID)
    );'''
    
    try:
        c.execute(query)
        print("Connection to table -tracking- successful")
    except Error as e:
        print(e)

def create_games_table(c):
    query = '''CREATE TABLE IF NOT EXISTS games (
        gameId integer PRIMARY KEY,
        gameDate text,
        gameTimeEastern text,
        homeTeamAbbr text,
        visitorTeamAbbr text,
        week real
    );'''
    
    try:
        c.execute(query)
        print("Connection to table -games- successful")
    except Error as e:
        print(e)
        
def create_player_table(c):
    query = '''CREATE TABLE IF NOT EXISTS players (
        nflId real PRIMARY KEY,
        height text,
        weight real,
        birthDate text,
        collegeName text,
        position text,
        displayName text
    );'''
    try:
        c.execute(query)
        print("Connection to table -games- successful")
    except Error as e:
        print(e)
        
def create_avg_separation_table(c):
    query = '''CREATE TABLE IF NOT EXISTS avg_separation (
        nflId real,
        gameId real,
        avgSeparation real,
        numPlays real,
        PRIMARY KEY (nflId, gameId)
    );'''
    
    try:
        c.execute(query)
        print("Connection to table -avg_separation- successful")
    except Error as e:
        print(e)

def create_avg_separation_by_player_table(c):
    query = '''CREATE TABLE IF NOT EXISTS avg_separation_by_player (
        nflId real PRIMARY KEY,
        avgSeparation real,
        numPlays real
    );'''
    
    try:
        c.execute(query)
        print("Connection to table -avg_separation_by_player- successful")
    except Error as e:
        print(e)

def create_avg_separation_and_epa_by_play_table(c):
    query = '''CREATE TABLE IF NOT EXISTS avg_separation_and_epa_by_play (
        playGUID integer PRIMARY KEY,
        epa real,
        avgSeparation real
    );'''
    try:
        c.execute(query)
        print("Connection to table -avg_separation_and_epa_by_play- successful")
    except Error as e:
        print(e)
        
def create_plays_table(c):
    query = '''CREATE TABLE IF NOT EXISTS plays (
        gameId integer,
        playId integer,
        playDescription text,
        quarter integer,
        down integer,
        yardsToGo integer,
        possessionTeam text,
        playType text,
        yardlineSide text,
        yardlineNumber real,
        offenseFormation text,
        personnelO text,
        defendersInTheBox integer,
        numberOfPassRushers integer,
        personnelD text,
        typeDropback text,
        preSnapHomeScore integer,
        preSnapVisitorScore integer,
        gameClock text,
        absoluteYardlineNumber integer,
        penaltyCodes text,
        penaltyJerseyNumbers text,
        passResult text,
        offensePlayResult text,
        playResult text,
        epa real,
        isDefensivePI text,
        PRIMARY KEY(gameId, playId)
    );'''
    
    try:
        c.execute(query)
        print("Connection to table -plays- successful")
    except Error as e:
        print(e)

def create_time_to_throw_vs_epa_table(c):
    query = '''CREATE TABLE IF NOT EXISTS time_to_throw_and_epa_by_play (
        playGUID integer PRIMARY KEY,
        epa real,
        timeToThrow real
    );'''
    try:
        c.execute(query)
        print("Connection to table -time_to_throw_and_epa_by_play- successful")
    except Error as e:
        print(e)
        
        
def create_target_receiver_table(c):
    query = '''CREATE TABLE IF NOT EXISTS targets (
        gameId integer,
        playId integer,
        targetNflId integer,
        PRIMARY KEY(gameId, playId)
    );'''
    try:
        c.execute(query)
        print("Connection to table -targets- successful")
    except Error as e:
        print(e)

def create_separation_vs_epa_by_play_table(c):
    query = '''CREATE TABLE IF NOT EXISTS separation_vs_epa_by_play (
        playGUID integer PRIMARY KEY,
        separation real,
        epa real
    );'''
    try:
        c.execute(query)
        print("Connection to table -separation_vs_epa_by_play- successful")
    except Error as e:
        print(e)

def create_zoi_completion_table(c):
    query = '''CREATE TABLE IF NOT EXISTS zoi_completion_by_play (
        nflId real,
        playId real,
        gameId real,
        inZoI integer,
        completed integer,
        PRIMARY KEY(nflId, playId, gameId)
    );'''
    try:
        c.execute(query)
        print("Connection to table -zoi_completion_by_play- successful")
    except Error as e:
        print(e)

def create_model_table(c):
    query = '''CREATE TABLE IF NOT EXISTS model_db (
        NflId real PRIMARY KEY,
        CompPer real,
        TopSpeed real,
        MaxAccel real,
        Height real,
        Weight real,
        Age integer,
        PredCompPer real,
        Residual real
    );'''
    try:
        c.execute(query)
        print("Connection to table -model_db- successful")
    except Error as e:
        print(e)            
##--------------------------------------------------------------------POPULATE TABLES------------------------------------------------------------------------##
def populate_games_table(c, data):
    try:
        for row in data:
            query = '''INSERT INTO games ( %s ) VALUES ( %s )''' % (unpack_list(list(row.keys())), unpack_list(list(row.values())))
            c.execute(query) 
        print("Inserted data succesfully")
    except Error as e:
        print(e)

def populate_plays_table(c, data):
    try:
        for row in data:
            query = '''INSERT INTO plays ( %s ) VALUES ( %s )''' % (unpack_list(list(row.keys())), unpack_list(list(row.values())))
            c.execute(query) 
        print("Inserted data succesfully")
    except Error as e:
        print(e)

def populate_tracking_table(c, data):
    counter = 0
    reader_count = 0
    try:
        for reader in data:
            for row in reader:
                query = '''INSERT OR IGNORE INTO tracking ( %s ) VALUES ( %s )''' % (unpack_list(list(row.keys())), unpack_list(list(row.values())))
                c.execute(query)
                counter += 1
            reader_count += 1
            print("Inserted row ( %d ) successfully" % reader_count)
        print("Inserted data succesfully: Inserted ( %d ) records" % counter)
    except Error as e:
        print(e)

def populate_targets_table(c, data):
    try:
        for row in data:
            query = '''INSERT INTO targets ( %s ) VALUES ( %s )''' % (unpack_list(list(row.keys())), unpack_list(list(row.values())))
            c.execute(query) 
        print("Inserted data succesfully")
    except Error as e:
        print(e)
 
def populate_players_table(c, data):
    try:
        for row in data:
            query = '''INSERT OR IGNORE INTO players ( %s ) VALUES ( %s )''' % (unpack_list(list(row.keys())), unpack_list(list(row.values())))
            c.execute(query) 
        print("Inserted data succesfully")
    except Error as e:
        print(e) 
        
def record_avg_separation_table(c, gameId, data):
    try:
        query = '''INSERT INTO avg_separation ( nflId, gameId, avgSeparation, numPlays ) VALUES ( %d, %d, %f, %d )''' % (data[0], gameId, data[1], data[2])
        c.execute(query) 
    except Error as e:
        print(e)

def record_avg_separation_by_player_table(c, data):
    try:
        query = '''INSERT INTO avg_separation_by_player ( nflId, avgSeparation, numPlays ) VALUES ( %d, %f, %d )''' % (data[0][0], data[1], data[2])
        c.execute(query) 
    except Error as e:
        print(e)

def record_avg_separation_and_epa_by_play(c, data):
    try:
        query = '''INSERT INTO avg_separation_and_epa_by_play ( playGUID, epa, avgSeparation ) VALUES ( %d, %f, %f )''' % (data[0], data[1], data[2])
        c.execute(query) 
    except Error as e:
        print(e)
        
def record_time_to_throw_vs_epa_by_play(c, data):
    try:
        query = '''INSERT INTO time_to_throw_and_epa_by_play ( playGUID, epa, timeToThrow ) VALUES ( %d, %f, %f )''' % (data[0], data[1], data[2])
        c.execute(query) 
    except Error as e:
        print(e)
        
def record_separation_vs_epa_by_play(c, data):
    try:
        query = '''INSERT INTO separation_vs_epa_by_play ( playGUID, separation, epa ) VALUES ( %d, %f, %f )''' % (data[0], data[1], data[2])
        c.execute(query) 
    except Error as e:
        print(e)
        
def record_zoi_completion_by_play(c, data):
    try:
        query = '''INSERT INTO zoi_completion_by_play ( nflId, playId, gameId, inZoI, completed ) VALUES ( %f, %f, %f, %d, %d )''' % (data[0], data[1], data[2], data[3], data[4])
        c.execute(query) 
    except Error as e:
        print(e)
        
def record_model_input_data(c, data_list):
    try:
        for data in data_list:
            query = '''INSERT INTO model_db ( NflId, CompPer, TopSpeed, MaxAccel, Height, Weight, Age ) VALUES ( %f, %f, %f, %f, %f, %f, %d )''' % (data[0], data[1], data[2], data[3], data[4], data[5], data[6])
            c.execute(query) 
    except Error as e:
        print(e)             
##------------------------------------------------------------------RETREIVAL METHODS------------------------------------------------------------------------##

def get_all_gameIds(c):
    gameIds = None
    try:
        query = '''SELECT DISTINCT gameId FROM games'''
        c.execute(query)
        gameIds = c.fetchall() 
        print("query executed successfully: ( %s ) records found" % (len(gameIds)))
    except Error as e:
        print(e)
    return gameIds

def get_nflIds_from_game(c, gameId):
    nflIds = None
    try:
        query = '''SELECT DISTINCT nflId FROM tracking WHERE gameId = ( %s ) AND position IN ("SS", "FS", "MLB", "LB", "CB")''' % (gameId)
        c.execute(query)
        nflIds = c.fetchall() 
        #print("query executed successfully: ( %s ) records found" % (len(nflIds)))
    except Error as e:
        print(e)
    return nflIds

def get_playIds_from_game(c, gameId):
    playIds = None
    try:
        query = '''SELECT DISTINCT playId FROM tracking WHERE gameId = ( %s )''' % (gameId)
        c.execute(query)
        playIds = c.fetchall() 
        #print("query executed successfully: ( %s ) records found" % (len(nflIds)))
    except Error as e:
        print(e)
    return playIds

def get_playIds_by_player(c, gameId, nflId):
    playIds = None
    try:
        query = '''SELECT DISTINCT playId FROM tracking WHERE gameId = ( %s ) AND nflId = ( %s )''' % (gameId, nflId)
        c.execute(query)
        playIds = c.fetchall() 
        #print("query executed successfully: ( %s ) records found" % (len(playIds)))
    except Error as e:
        print(e)
    return playIds

def get_frameIds_by_play(c, gameId, nflId, playId, stop_frame_id):
    frameIds = None
    try:
        query = '''SELECT DISTINCT frameId FROM tracking WHERE gameId = ( %s ) AND nflId = ( %s ) AND playId = ( %s ) AND frameId <= ( %d )''' % (gameId, nflId, playId, stop_frame_id)
        c.execute(query)
        frameIds = c.fetchall() 
        #print("query executed successfully: ( %s ) records found" % (len(frameIds)))
    except Error as e:
        print(e)
    return frameIds

def get_locations_by_frame(c, gameId, playId, frameId):
    locations = None
    try:
        query = '''SELECT x, y, nflId FROM tracking WHERE gameId = ( %s ) AND  playId = ( %s ) AND frameId = ( %s ) AND position IN ("WR", "TE", "RB")''' % (gameId, playId, frameId)
        c.execute(query)
        locations = c.fetchall() 
        #print("query executed successfully: ( %s ) records found" % (len(locations)))
    except Error as e:
        print(e)
    return locations

def get_target_defender_location(c, gameId, playId, frameId, targetDefId):
    location = None
    try:
        query = '''SELECT x, y, nflId FROM tracking WHERE gameId = ( %s ) AND  playId = ( %s ) AND frameId = ( %s ) AND nflId = ( %s )''' % (gameId, playId, frameId, targetDefId)
        c.execute(query)
        location = c.fetchall() 
        #print("query executed successfully: ( %s ) records found" % (len(location)))
    except Error as e:
        print(e)
    return location

def get_name_by_nflId(c, nflId):
    name = None
    try:
        query = '''SELECT displayName FROM tracking WHERE nflId = ( %s )''' % (nflId)
        c.execute(query)
        name = c.fetchall()[0][0] 
        #print("query executed successfully: ( %s ) records found" % (len(location)))
    except Error as e:
        print(e)
    return name

def get_frameId_where_pass_arrives(c, gameId, playId):
    frameId = None
    try:
        query = '''SELECT DISTINCT frameId FROM tracking WHERE gameId = ( %s ) AND playId = ( %s ) AND event IN ("pass_outcome_caught", "pass_outcome_incomplete", "pass_outcome_interception", "pass_outcome_touchdown", "qb_sack", "qb_strip_sack", "qb_spike", "tackle") ''' % (gameId, playId)
        c.execute(query)
        arr = c.fetchall()
        if arr:
            frameId = arr[0]
        else:
            return 1
        #print("query executed successfully: ( %s ) records found" % (len(frameIds)))
    except Error as e:
        print(e)
    return int(frameId[0])

def get_nflIds_from_separation_table(c):
    nflIds = None
    try:
        query = '''SELECT DISTINCT nflId FROM avg_separation'''
        c.execute(query)
        nflIds = c.fetchall() 
        #print("query executed successfully: ( %s ) records found" % (len(nflIds)))
    except Error as e:
        print(e)
    return nflIds

def get_separation_for_player_in_separation_table(c, nflId):
    separations = None
    try:
        query = '''SELECT avgSeparation, numPlays FROM avg_separation WHERE nflId = ( %d )''' % (nflId)
        c.execute(query)
        separations = c.fetchall() 
    except Error as e:
        print(e)
    return separations

def get_epa_by_play(c):
    plays = None
    try:
        query = '''SELECT gameId, playId, epa FROM plays'''
        c.execute(query)
        plays = c.fetchall() 
    except Error as e:
        print(e)
    return plays

def get_first_frame_of_play(c, gameId, playId):
    frame = None
    try:
        query = '''SELECT frameId FROM tracking WHERE gameId = ( %d ) AND playId = ( %d ) LIMIT 1''' % (gameId, playId)
        c.execute(query)
        frame = c.fetchall()
    except Error as e:
        print(e)
    return frame

def get_nflIds_from_play(c, gameId, playId, frameId):
    nflIds = None
    try:
        query = '''SELECT nflId FROM tracking WHERE gameId = ( %d ) AND playId = ( %d ) AND frameId = ( %s ) AND position IN ("SS", "FS", "MLB", "LB", "CB")''' % (gameId, playId, frameId)
        c.execute(query)
        nflIds = c.fetchall() 
    except Error as e:
        print(e)
    return nflIds

def get_epa_and_avg_separation_by_play(c):
    plays = None
    try:
        query = '''SELECT * FROM avg_separation_and_epa_by_play'''
        c.execute(query)
        plays = c.fetchall() 
    except Error as e:
        print(e)
    return plays

def get_num_pass_rushers_and_epa_by_play(c):
    plays = None
    try:
        query = '''SELECT numberOfPassRushers, epa FROM plays WHERE numberOfPassRushers <> ""'''
        c.execute(query)
        plays = c.fetchall() 
    except Error as e:
        print(e)
    return plays


def get_frameId_where_ball_snapped(c, gameId, playId):
    plays = None
    try:
        query = '''SELECT frameId, time FROM tracking WHERE gameId = ( %d ) AND playId = ( %d ) AND event = "ball_snap" LIMIT 1''' % (gameId, playId)
        c.execute(query)
        plays = c.fetchall() 
    except Error as e:
        print(e)
    return plays

def get_frameId_and_time_where_pass_attempted(c, gameId, playId):
    frameId = None
    try:
        query = '''SELECT frameId, time FROM tracking WHERE gameId = ( %s ) AND playId = ( %s ) AND event IN ("pass_lateral", "pass_forward") LIMIT 1''' % (gameId, playId)
        c.execute(query)
        arr = c.fetchall()
        if arr:
            frameId = arr[0]
        else:
            return 1
        #print("query executed successfully: ( %s ) records found" % (len(frameIds)))
    except Error as e:
        print(e)
    return frameId

def get_time_to_throw_and_epa_by_play(c):
    plays = None
    try:
        query = '''SELECT playGUID, timeToThrow, epa FROM time_to_throw_and_epa_by_play WHERE timeToThrow > 0'''
        c.execute(query)
        plays = c.fetchall() 
    except Error as e:
        print(e)
    return plays

def get_target_receiver_by_play(c, gameId, playId):
    target = None
    try:
        query = '''SELECT targetNflId FROM targets WHERE gameId = ( %d ) AND playId = ( %d ) AND targetNflId <> "NA"''' % (gameId, playId)
        c.execute(query)
        target = c.fetchall() 
    except Error as e:
        print(e)
    return target

def get_target_receiver_location(c, gameId, playId, frameId, nflId):
    target = None
    try:
        query = '''SELECT nflId, x, y FROM tracking WHERE gameId = ( %d ) AND playId = ( %d ) AND frameId = ( %d ) AND nflId = ( %d )''' % (gameId, playId, frameId, nflId)
        c.execute(query)
        target = c.fetchall()
        if target == []:
            return -1
    except Error as e:
        print(e)
    return target

def get_separation_and_epa_by_play(c):
    target = None
    try:
        query = '''SELECT * FROM separation_vs_epa_by_play'''
        c.execute(query)
        target = c.fetchall() 
    except Error as e:
        print(e)
    return target

def get_yards_gained_by_play(c, gameId, playId):
    yards = None
    try:
        query = '''SELECT offensePlayResult FROM plays WHERE gameId = ( %d ) AND playId = ( %d ) AND playType <> "play_type_sack"''' % (gameId, playId)
        c.execute(query)
        yards = c.fetchall() 
    except Error as e:
        print(e)
    return yards

def get_separation_and_time_to_throw_by_play(c):
    play = None
    try:
        query = '''select ti.timeToThrow, sep.separation from time_to_throw_and_epa_by_play ti inner join separation_vs_epa_by_play sep on ti.playGUID = sep.playGUID WHERE ti.timeToThrow > 0'''
        c.execute(query)
        play = c.fetchall() 
    except Error as e:
        print(e)
    return play

def get_ball_location(c, gameId, playId, frameId):
    loc = None
    try:
        query = '''SELECT x, y from tracking where gameId = ( %d ) and playId = ( %d ) and frameID = ( %d ) and displayName = "Football"''' % (gameId, playId, frameId)
        c.execute(query)
        loc = c.fetchall() 
    except Error as e:
        print(e)
    return loc    

def get_is_pass_completed(c, gameId, playId):
    result = None
    try:
        query = '''SELECT distinct frameID, CASE WHEN event IN ("pass_outcome_caught", "pass_outcome_touchdown") THEN 1 WHEN event IN ("pass_outcome_incomplete", "pass_outcome_interception") THEN 0 ELSE -1 END AS result FROM tracking WHERE gameId = ( %d ) AND playId = ( %d ) AND result <> -1''' % (gameId, playId)
        c.execute(query)
        result = c.fetchall() 
    except Error as e:
        print(e)
    return result

def get_zoi_players(c, min_plays):
    nflIds = None
    try:
        query = '''SELECT DISTINCT nflId, count(nflId) from zoi_completion_by_play where inZoI = 1 GROUP by nflId'''
        c.execute(query)
        nflIds = c.fetchall() 
    except Error as e:
        print(e)
    return list(filter(lambda player: player[1] > min_plays, nflIds))  

def get_zoi_plays_by_player(c, nflId):
    plays = None
    try:
        query = '''SELECT completed FROM zoi_completion_by_play WHERE nflId = ( %r ) AND inZoI = 1''' % (nflId)
        c.execute(query)
        plays = c.fetchall() 
    except Error as e:
        print(e)
    return plays    

def get_name_by_nfl(c, nflId):
    name = None
    try:
        query = '''SELECT displayName FROM tracking WHERE nflId = ( %r ) LIMIT 1''' % (nflId)
        c.execute(query)
        name = c.fetchall() 
    except Error as e:
        print(e)
    return name

def get_top_speed_by_nflId(c, nflId):
    name = None
    try:
        query = '''SELECT s from tracking where nflId = ( %r ) ORDER BY s DESC LIMIT 1''' % (nflId)
        c.execute(query)
        name = c.fetchall() 
    except Error as e:
        print(e)
    return name    


def get_max_accel_by_nflId(c, nflId):
    name = None
    try:
        query = '''SELECT a from tracking where nflId = ( %r ) ORDER BY a DESC LIMIT 1''' % (nflId)
        c.execute(query)
        name = c.fetchall() 
    except Error as e:
        print(e)
    return name   

def get_playerinfo_by_nflId(c, nflId):
    name = None
    try:
        query = '''SELECT height, weight, birthDate from players where nflId = ( %r )''' % (nflId)
        c.execute(query)
        name = c.fetchall() 
    except Error as e:
        print(e)
    return name

def get_modelinfo_by_nflId(c, nflIds):
    data_list = []
    try:
        for nflId in nflIds:
            query = '''SELECT NflId, CompPer, TopSpeed, MaxAccel, Height, Weight, Age from model_db where NflId = ( %r )''' % (nflId)
            c.execute(query)
            data = c.fetchall()[0]
            data_list.append(data) 
    except Error as e:
        print(e)
    return data_list      
##--------------------------------------------------------------------DROP TABLE METHODS---------------------------------------------------------------------##
def drop_games_table(c):
    query = '''DROP TABLE games;'''
    try:
        c.execute(query)
        print("Drop table -games- successful")
    except Error as e:
        print(e)

def drop_plays_table(c):
    query = '''DROP TABLE plays;'''
    try:
        c.execute(query)
        print("Drop table -plays- successful")
    except Error as e:
        print(e)
        
def drop_tracking_table(c):
    query = '''DROP TABLE tracking;'''
    try:
        c.execute(query)
        print("Drop table -tracking- successful")
    except Error as e:
        print(e)
        
def drop_targets_table(c):
    query = '''DROP TABLE targets;'''
    try:
        c.execute(query)
        print("Drop table -targets- successful")
    except Error as e:
        print(e)
               
def drop_separation_table(c):
    query = '''DROP TABLE avg_separation;'''
    try:
        c.execute(query)
        print("Drop table -avg_separation- successful")
    except Error as e:
        print(e)
        
def drop_separation_by_player_table(c):
    query = '''DROP TABLE avg_separation_by_player;'''
    try:
        c.execute(query)
        print("Drop table -avg_separation_by_player- successful")
    except Error as e:
        print(e)
        
def drop_time_to_throw_and_epa_by_play_table(c):
    query = '''DROP TABLE time_to_throw_and_epa_by_play;'''
    try:
        c.execute(query)
        print("Drop table -time_to_throw_and_epa_by_play- successful")
    except Error as e:
        print(e)

def drop_zoi_completion_by_play_table(c):
    query = '''DROP TABLE zoi_completion_by_play;'''
    try:
        c.execute(query)
        print("Drop table -zoi_completion_by_play- successful")
    except Error as e:
        print(e)
        
##----------------------------------------------------------------UTILITY METHODS----------------------------------------------------------------------------##
def unpack_list(data_list):
    data_string = '"' + '", "'.join(data_list) + '"'
    return data_string
