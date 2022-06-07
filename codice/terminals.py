import numpy as np
import pandas as pd
from pandas import DataFrame
import time

from pydantic import TimeError

class Terminal:
    dataset: DataFrame
    gen_time: float

    def __init__(self, gen, n_terminals=0, path=""):
        if gen:
            self.generate_profiles_table(n_terminals)
        else:
            self.dataset = pd.read_pickle(path)
    
    def generate_profiles_table(self, n_terminals, random_state=0):
        self.gen_time = time.time()
        np.random.seed(random_state)
        terminal_id_properties=[]
        for terminal_id in range(n_terminals):
            x_terminal_id = np.random.uniform(0,100)
            y_terminal_id = np.random.uniform(0,100)
            terminal_id_properties.append([terminal_id,
                                        x_terminal_id, y_terminal_id])
                                        
        self.dataset = pd.DataFrame(terminal_id_properties, columns=['TERMINAL_ID',
                                                                        'x_terminal_id', 'y_terminal_id'])

        self.gen_time = time.time()-self.gen_time
               
        
    def get_dataset(self):
        print(self.dataset)
    
    def get_list_terminals_within_radius(customer_profile, x_y_terminals, r):
        x_y_customer = customer_profile[['x_customer_id','y_customer_id']].values.astype(float)
        squared_diff_x_y = np.square(x_y_customer - x_y_terminals)
        dist_x_y = np.sqrt(np.sum(squared_diff_x_y, axis=1))
        available_terminals = list(np.where(dist_x_y<r)[0])
        
        return available_terminals
