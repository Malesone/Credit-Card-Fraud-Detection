import os

import numpy as np
import pandas as pd

import seaborn as sns
import sys

from sqlalchemy import null

class Generation: 
    customers: pd.DataFrame
    terminals: pd.DataFrame
    
    def __init__(self, n_customers, n_terminals):
        np.random.seed(0) 
        self.generate_customer(n_customers)
        self.generate_terminals(n_terminals)
    
    def generate_customer(self, n_customers):
        customer_id_properties=[]
        for customer_id in range(n_customers):
            x_customer_id = np.random.uniform(0,100)
            y_customer_id = np.random.uniform(0,100)
            mean_amount = np.random.uniform(5,100) 
            std_amount = mean_amount/2 
            mean_nb_tx_per_day = np.random.uniform(0,4) 
            customer_id_properties.append([customer_id,
                                        x_customer_id, y_customer_id,
                                        mean_amount, std_amount,
                                        mean_nb_tx_per_day])
            
        customer_profiles_table = pd.DataFrame(customer_id_properties, columns=['CUSTOMER_ID',
                                                                        'x_customer_id', 'y_customer_id',
                                                                        'mean_amount', 'std_amount',
                                                                        'mean_nb_tx_per_day'])
        self.customers = customer_profiles_table

    def generate_terminals(self, n_terminals):
        terminal_id_properties=[]

        for terminal_id in range(n_terminals):
            x_terminal_id = np.random.uniform(0,100)
            y_terminal_id = np.random.uniform(0,100)
            terminal_id_properties.append([terminal_id,
                                        x_terminal_id, y_terminal_id])
                                        
        terminal_profiles_table = pd.DataFrame(terminal_id_properties, columns=['TERMINAL_ID',
                                                                        'x_terminal_id', 'y_terminal_id'])

        self.terminals = terminal_profiles_table