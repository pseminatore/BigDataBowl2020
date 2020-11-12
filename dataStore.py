import sqlite3
from sqlite3 import Error
import os

## --------------------CREATE DATABASE AND CONNECTION--------------------------------------------------##
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

##--------------------CREATE TABLES-------------------------------------------------------------------##
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


##----------------------POPULATE TABLES---------------------------------------------------##
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
##---------------------------------RETREIVAL METHODS---------------------------------##

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

##-------------------------DROP TABLE METHODS-----------------------------------------##
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
        
##--------------------------------UTILITY METHODS-----------------------------------##
def unpack_list(data_list):
    data_string = '"' + '", "'.join(data_list) + '"'
    return data_string
