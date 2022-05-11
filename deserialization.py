import pickle
import pandas as pd
import os
import csv

folder = 'simulated-data-raw-github/'
files = []
dir = os.path.join('.','csv-github')

with os.scandir(folder) as entries:
    for entry in entries:
        name = entry.name
        files.append(name)

files.sort()

#if os.path.exists(folder+'.DS_Store'):
#    os.remove(folder+'.DS_Store')

for entry in files: 
    name = folder+entry
    with open(name, 'rb') as f:
        data = pickle.load(f)
        if not os.path.exists(dir):
            os.mkdir(dir)    
        nome = './csv-github/'+entry.replace(".pkl", ".csv")
        df = pd.DataFrame(data)
        df.to_csv(nome, index=False)