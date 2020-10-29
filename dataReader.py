import csv
import glob
import os

def readWeek1TrackingData():
    with open("Data/Tracking/week1.csv") as csvfile:
        reader = csv.DictReader(csvfile)
        return reader
    
def readTrackingData():
    data = []
    for filename in glob.glob("Data/Tracking/week*.csv"):
        with open(os.path.join(os.getcwd(), filename)) as csvfile:
            reader = csv.DictReader(csvfile)
            data.append(reader)
            
def readPlayerData():
    data = []
    for filename in glob.glob("Data/players.csv"):
        with open(os.path.join(os.getcwd(), filename)) as csvfile:
            reader = csv.DictReader(csvfile)
            data.append(reader)
            
def readPlayData():
    data = []
    for filename in glob.glob("Data/plays.csv"):
        with open(os.path.join(os.getcwd(), filename)) as csvfile:
            reader = csv.DictReader(csvfile)
            data.append(reader)
            
                
if __name__ == "__main__":
    readTrackingData()

