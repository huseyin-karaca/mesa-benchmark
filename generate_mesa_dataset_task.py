import pandas as pd
import numpy as np
# required by pd.read_csv. It expects an object with a .read() method.
from datetime import datetime, timedelta
from glob import glob
from bs4 import BeautifulSoup
import gzip
import os 
import xml.etree.ElementTree as ET


def getOverlap(mesaid, overlap_file="./data/mesa/overlap/mesa-actigraphy-psg-overlap.csv"):
    
    df = pd.read_csv(overlap_file)
    df = df[df["mesaid"] == mesaid]
    if df.empty:
        print("ERROR: mesaid %d not found in overlap file" % (mesaid))
        return None
    return df.values[0][1] # (mesaid, line, timeactigraph, timepsg)

def extractAcigraphy(mesaid):
    # Start to extract Actigraphy actValue
    try:
        act_file = "data/mesa/actigraphy/mesa-sleep-%04d.csv" % (mesaid)
        df = load_mesa(act_file)
        return df
    except:
        return None

def extractPSG(mesaid):
    # Start to extract PSG sleep scores
    profusion_file = "/home/huseyin/bitirme/mesa-benchmark/data/mesa/polysomnography/annotations-events-profusion/mesa-sleep-%04d-profusion.xml" % (mesaid)

    with open(profusion_file, 'r') as f:
        data = f.read() 

    soup = BeautifulSoup(data, "xml")
    print("BİR GELİŞME OLDU")

    stages = []
    for c in soup.CMPStudyConfig.SleepStages:
        stages.append(c.string)

    return stages

def extractPSG2(mesaid):
    # Start to extract PSG sleep scores
    profusion_file = "/home/huseyin/bitirme/mesa-benchmark/data/mesa/polysomnography/annotations-events-profusion/mesa-sleep-%04d-profusion.xml" % (mesaid)

    try:
        tree = ET.parse(profusion_file)
        root = tree.getroot()

        stages = []

        # for sleep_stage in root.findall(".//CMPStudyConfig/SleepStages/*"):
        for sleep_stage in [1,2,3,4,5]:
            stages.append(sleep_stage.text)

        return stages

    except Exception as e:
        print(f"Error: {e}")
        return None
    

def load_mesa(filename):
    """
        Load all the mesa files
    """
    
    df = pd.read_csv(filename, index_col="line")
    
    ts = get_timestamp("1/1/1900", df.iloc[0]["linetime"])
    pts = pd.Timestamp(ts)
    df["linetime"] = pd.date_range(pts, periods=df.shape[0], freq='30S')
    
    return df

def get_timestamp(start_date, start_time):
    return datetime.strptime(start_date + " " + start_time, '%m/%d/%Y %H:%M:%S')

def get_time_interval(n):
    minutes = n / 2
    hours = minutes/ 60
    rest_minutes = minutes - (hours * 60)
    rest_seconds = "30" if n%2 == 1 else "00"
    return "%02d:%02d:%s" % (hours, rest_minutes, rest_seconds)

def process(mesaid, task):
    """
    - mesaid: Integer representing a valid mesaid.
    
    - task:
         * task1 (PSG time)
         * task2 (24 hours)
    """
    print("Processing %d..." % (mesaid))
    
    # Extract Actigraphy data
    df = extractAcigraphy(mesaid)
    if df is None:
        print ("Actigraphy not found for mesaid %d. Aborting..." % (mesaid))
        return None
    
    # Extract PSG data
    stages = extractPSG2(mesaid)
    if stages is None:
        print ("PSG not found for mesaid %d. Aborting..." % (mesaid))
        return None
    
    stages = list(map(int, stages))

    # If recording for more than 16 hours, we do not use this mesaid
    if len(stages) > 1920:
        print ("PSG recording was longer than 16 hours for mesaid %d." % (mesaid))
        print ("Aborting...")
        return None
    
    # Extract overlap index
    overlapidx = getOverlap(mesaid)

    if overlapidx is None:
        print ("Problems with mesaid %d. Aborting..." % (mesaid))
        return None
    
    #return df, stages, overlapidx
    
    # Creates an extra col with PSG data and fills up data with stages
    df["stage"] = None
    df.loc[overlapidx:overlapidx+len(stages)-1, "stage"] = stages
    
    # Task 1: only PSG data is kept
    if task == 1:
        df = df[~df["stage"].isnull()]
        
    elif task == 2: # aims to get 960 (8 hours) intervals before and after PSG 
        startidx = max(0, overlapidx - 960)
        endidx = min(df.shape[0], overlapidx+len(stages)-1 + 960)
        df = df.loc[startidx: endidx]
    
    # Generates the ground truth data:
    if task == 1:
        df["gt"] = (df["stage"] > 0)
    
    elif task == 2:
        # ...uses GT as the PSG states and uses "interval" data for the rest....
        df["gt"] = None
        beforePSG = df.loc[:overlapidx-1].copy()
        duringPSG = df.loc[overlapidx: overlapidx+len(stages)-2].copy()
        afterPSG = df.loc[overlapidx+len(stages)-1:].copy()

        beforePSG["gt"] = beforePSG["interval"].apply(lambda x: x in ["REST", "REST-S"])
        afterPSG["gt"] = afterPSG["interval"].apply(lambda x: x in ["REST", "REST-S"])
        duringPSG["gt"] =  (duringPSG["stage"] > 0)
        df = pd.concat((beforePSG,duringPSG,afterPSG))
        
    df["gt"] = df["gt"].astype(int)
        
    dflenght = df.shape[0]
    print("Final df duration %s (%d intervals) -- From %s to %s" % (get_time_interval(dflenght),dflenght, df.head(1)["linetime"].values[0], df.tail(1)["linetime"].values[0]))

    # Resets index
    df = df.reset_index(drop=True)
    
    return df
    
#process(1, 2)  # 2896, 4012 should be there...



TASK = 2
outdir = "data/mesa/processed/task%d" % TASK

variables_file = "./data/mesa/datasets/mesa-sleep-dataset-0.6.0.csv"
variables = pd.read_csv(variables_file)
ids = list(variables["mesaid"].unique())

problem = []
okays = 0
empties = []
processed = 0

for mesaid in ids[:]:
    
    processed += 1
    
    print("*" * 80)    
    df = process(mesaid, task=TASK)
    print("*" * 80)
    
    if df is None:
        print("Could not get data for mesaid %d" % (mesaid))
        problem.append(mesaid)
        continue
    
    if df[(df["interval"] == "EXCLUDED")].shape[0] > 0:
        print("FOUND %d rows to be excluded" % (df[(df["interval"] == "EXCLUDED")].shape[0]))
    
    if (df[df["interval"] == "EXCLUDED"].shape[0] == df.shape[0]) or df.empty:
        print("ERROR: All intervals were excluded.")
        print("MesaID %d is empty..." % mesaid)
        empties.append(mesaid)
        continue
    
    #df = df[df["interval"] != "EXCLUDED"] # TODO: not sure if I should keep or remove the excluded rows.
   
    outfile = "mesa_%000d_task%s.csv" % (mesaid, TASK)
    outpath = os.path.join(outdir, outfile)
        
    df.to_csv(outpath, index=False)
    okays += 1
    
print("Done with %d okay out of %d possible ids" % (okays, processed))
