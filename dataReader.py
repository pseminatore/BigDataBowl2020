import csv
import glob
import os

def readWeek1TrackingData():
    csvfile =  open("Data/Tracking/week1.csv")
    reader = csv.DictReader(csvfile)
    return reader
    
def readTrackingData():
    data = []
    for filename in glob.glob("Data/Tracking/week*.csv"):
        csvfile = open(os.path.join(os.getcwd(), filename))
        reader = csv.DictReader(csvfile)
        data.append(reader)
    return data
            
def readPlayerData():
    data = []
    for filename in glob.glob("Data/players.csv"):
        csvfile = open(os.path.join(os.getcwd(), filename))
        reader = csv.DictReader(csvfile)
        data.append(reader)
    return data
            
def readPlayData():
    data = []
    for filename in glob.glob("Data/plays.csv"):
        csvfile = open(os.path.join(os.getcwd(), filename))
        reader = csv.DictReader(csvfile)
        data.append(reader)
    return data
            
def readGameData():
    data = []
    for filename in glob.glob("Data/games.csv"):
        csvfile = open(os.path.join(os.getcwd(), filename))
        reader = csv.DictReader(csvfile)
        data.append(reader)
    return data
                       
                
if __name__ == "__main__":
    readTrackingData()

