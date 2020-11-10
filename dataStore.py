import sqlite3
from sqlite3 import Error
import os


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
        nflId real PRIMARY KEY,
        gameId real,
        avgSeparation real,
    );'''
    
    try:
        c.execute(query)
        print("Connection to table -avg_separation- successful")
    except Error as e:
        print(e)

def populate_games_table(c, data):
    try:
        for row in data:
            query = '''INSERT INTO games ( %s ) VALUES ( %s )''' % (unpack_list(list(row.keys())), unpack_list(list(row.values())))
            c.execute(query) 
        print("Inserted data succesfully")
    except Error as e:
        print(e)

def populate_tracking_table(c, data):
    counter = 0
    reader_count = 0
    try:
        for reader in data[:2]:
            for row in reader:
                query = '''INSERT INTO tracking ( %s ) VALUES ( %s )''' % (unpack_list(list(row.keys())), unpack_list(list(row.values())))
                c.execute(query)
                counter += 1
            reader_count += 1
            print("Inserted row ( %d ) successfully" % reader_count)
        print("Inserted data succesfully: Inserted ( %d ) records" % counter)
    except Error as e:
        print(e)

def unpack_list(data_list):
    data_string = '"' + '", "'.join(data_list) + '"'
    return data_string

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
        print("query executed successfully: ( %s ) records found" % (len(nflIds)))
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
        frameId = c.fetchall()[0] 
        #print("query executed successfully: ( %s ) records found" % (len(frameIds)))
    except Error as e:
        print(e)
    return int(frameId[0])

def drop_games_table(c):
    query = '''DROP TABLE games;'''
    try:
        c.execute(query)
        print("Drop table -games- successful")
    except Error as e:
        print(e)
        
def drop_tracking_table(c):
    query = '''DROP TABLE tracking;'''
    try:
        c.execute(query)
        print("Drop table -tracking- successful")
    except Error as e:
        print(e)
