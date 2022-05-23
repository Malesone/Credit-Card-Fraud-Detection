import numpy as np
import pandas as pd

import random
from enum import Enum

class Moment(Enum):
    morning = "morning"
    afternoon = "afternoon"
    evening = "evening"
    night = "night"

def generate_transactions_table(customer_profile, start_date = "2022-01-01", nb_days = 10):
    product = ['high-tech', 'food', 'clothing', 'consumable', 'other']
    customer_transactions = []
    
    random.seed(int(customer_profile.CUSTOMER_ID))
    np.random.seed(int(customer_profile.CUSTOMER_ID))
    
    # For all days
    for day in range(nb_days):
        
        # Random number of transactions for that day 
        nb_tx = np.random.poisson(customer_profile.mean_nb_tx_per_day)
        
        # If nb_tx positive, let us generate transactions
        if nb_tx>0:
            
            for tx in range(nb_tx):
                time_tx = int(np.random.normal(86400/2, 20000))
                
                if (time_tx>0) and (time_tx<86400):
                    #morning = 6-12
                    #afternoon = 12-17
                    #evening = 17-22
                    #night = 22-6
                    
                    if(time_tx>=21600 and time_tx<43200):
                        moment = Moment.morning
                    elif(time_tx>=43200 and time_tx<61200):
                        moment = Moment.afternoon
                    elif(time_tx>=61200 and time_tx<79200):
                        moment = Moment.evening
                    else:
                        moment = Moment.night

                    # Amount is drawn from a normal distribution  
                    amount = np.random.normal(customer_profile.mean_amount, customer_profile.std_amount)
                    
                    # If amount negative, draw from a uniform distribution
                    if amount<0:
                        amount = np.random.uniform(0,customer_profile.mean_amount*2)
                    
                    amount=np.round(amount,decimals=2)
                    
                    if len(customer_profile.available_terminals)>0:
                        
                        terminal_id = random.choice(customer_profile.available_terminals)
                    
                        customer_transactions.append([time_tx+day*86400, day,
                                                      customer_profile.CUSTOMER_ID, 
                                                      terminal_id, amount, moment.value, random.choice(product)])
            
    customer_transactions = pd.DataFrame(customer_transactions, columns=['TX_TIME_SECONDS', 'TX_TIME_DAYS', 'CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT', 'MOMENT', 'PRODUCT'])
    
    if len(customer_transactions)>0:
        customer_transactions['TX_DATETIME'] = pd.to_datetime(customer_transactions["TX_TIME_SECONDS"], unit='s', origin=start_date)
        customer_transactions=customer_transactions[['TX_DATETIME','CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT','TX_TIME_SECONDS', 'TX_TIME_DAYS', 'MOMENT', 'PRODUCT']]
    
    return customer_transactions  