import pickle
import pandas as pd
import os
import csv

folder = './simulated-data-raw-github/'
files = []

with os.scandir(folder) as entries:
    for entry in entries:
        name = entry.name
        files.append(name)

files.sort()
for entry in files: 
    name = folder+entry
    with open(name, 'rb') as f:
        newName = entry.replace(".pkl", ".csv")
        print(newName)
        data = pickle.load(f)
        #nome = './csv-github/'+entry.name.replace(".pkl", ".csv")
        #df = pd.DataFrame(data)
        #df.to_csv(nome, index=False)
 