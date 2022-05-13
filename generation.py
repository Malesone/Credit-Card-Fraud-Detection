import os
from matplotlib.style import available

import numpy as np
import pandas as pd

import seaborn as sns
import sys

from sqlalchemy import null

import matplotlib.pyplot as plt
import seaborn as sns

import random

class Generation: 
    customers: pd.DataFrame
    terminals: pd.DataFrame
    x_y_terminals: np.ndarray
    available_terminals: list

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

    def get_list_terminals_within_radius(self, customer_profile, x_y_terminals, r):
        # Location (x,y) of customer as numpy array
        x_y_customer = customer_profile[['x_customer_id','y_customer_id']].values.astype(float)    
        # Squared difference in coordinates between customer and terminal locations
        squared_diff_x_y = np.square(x_y_customer - x_y_terminals)
        # Sum along rows and compute suared root to get distance
        dist_x_y = np.sqrt(np.sum(squared_diff_x_y, axis=1))
        # Get the indices of terminals which are at a distance less than r
        self.available_terminals = list(np.where(dist_x_y<r)[0])
        # Return the list of terminal IDs
        
    def draw(self):
        self.get_geographical_locations()

        terminals_available_to_customer_fig, ax = plt.subplots(figsize=(5,5))

        # Plot locations of terminals
        ax.scatter(self.terminals.x_terminal_id.values, 
                self.terminals.y_terminal_id.values, 
                color='blue', label = 'Locations of terminals')

        # Plot location of the last customer
        customer_id=4
        ax.scatter(self.customers.iloc[customer_id].x_customer_id, 
                self.customers.iloc[customer_id].y_customer_id, 
                color='red',label="Location of last customer")

        ax.legend(loc = 'upper left', bbox_to_anchor=(1.05, 1))

        # Plot the region within a radius of 50 of the last customer
        circ = plt.Circle((self.customers.iloc[customer_id].x_customer_id,
                        self.customers.iloc[customer_id].y_customer_id), radius=50, color='g', alpha=0.2)
        ax.add_patch(circ)

        fontsize=15

        ax.set_title("Green circle: \n Terminals within a radius of 50 \n of the last customer")
        ax.set_xlim([0, 100])
        ax.set_ylim([0, 100])
            
        ax.set_xlabel('x_terminal_id', fontsize=fontsize)
        ax.set_ylabel('y_terminal_id', fontsize=fontsize)

        return terminals_available_to_customer_fig

    def get_geographical_locations(self):
        # We first get the geographical locations of all terminals as a numpy array
        self.x_y_terminals = self.terminals[['x_terminal_id','y_terminal_id']].values.astype(float)
        # And get the list of terminals within radius of $50$ for the last customer
        self.get_list_terminals_within_radius(self.customers.iloc[4], x_y_terminals=self.x_y_terminals, r=50)
    
    #aggiungo ai customer i terminali vicini a loro
    def set_available_terminals(self):
        self.customers['available_terminals']=self.customers.apply(lambda x : self.get_list_terminals_within_radius(x, x_y_terminals=self.x_y_terminals, r=50), axis=1)
 