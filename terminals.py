import os

import numpy as np
import pandas as pd

import seaborn as sns
import sys

def generate_terminal_profiles_table(n_terminals, random_state=0):
    
    np.random.seed(random_state)
        
    terminal_id_properties=[]
    
    # Generate terminal properties from random distributions 
    for terminal_id in range(n_terminals):
        
        x_terminal_id = np.random.uniform(0,100)
        y_terminal_id = np.random.uniform(0,100)
        
        terminal_id_properties.append([terminal_id,
                                      x_terminal_id, y_terminal_id])
                                       
    terminal_profiles_table = pd.DataFrame(terminal_id_properties, columns=['TERMINAL_ID',
                                                                      'x_terminal_id', 'y_terminal_id'])
    
    return terminal_profiles_table

n_terminals = 1050
terminal_profiles_table = generate_terminal_profiles_table(n_terminals, random_state = 0)
print(terminal_profiles_table)

sizeByte = sys.getsizeof(terminal_profiles_table)
sizeMB = sizeByte/1024

print("Size: ", sizeMB, "MB")