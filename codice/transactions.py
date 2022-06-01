import numpy as np
import pandas as pd
import random
from enum import Enum
from pandas import DataFrame
import time 

class Moment(Enum):
    morning = "morning"
    afternoon = "afternoon"
    evening = "evening"
    night = "night"

class Transaction:
    dataset: DataFrame
    gen_time: float

    def generate_transactions_table(self, customer_profile, nb_days, start_date = "2022-01-01"):
        self.gen_time = time.time()
        product = ['high-tech', 'food', 'clothing', 'consumable', 'other']
        customer_transactions = []
        random.seed(int(customer_profile.CUSTOMER_ID))
        np.random.seed(int(customer_profile.CUSTOMER_ID))
        
        for day in range(nb_days):
            nb_tx = np.random.poisson(customer_profile.mean_nb_tx_per_day)
            if nb_tx>0:
                
                for tx in range(nb_tx):
                    time_tx = int(np.random.normal(86400/2, 20000))
                    
                    if (time_tx>0) and (time_tx<86400):
                        #morning = 6-12
                        #afternoon = 12-17
                        #evening = 17-22
                        #night = 22-6
                        
                        """if(time_tx>=21600 and time_tx<43200):
                            moment = Moment.morning
                        elif(time_tx>=43200 and time_tx<61200):
                            moment = Moment.afternoon
                        elif(time_tx>=61200 and time_tx<79200):
                            moment = Moment.evening
                        else:
                            moment = Moment.night"""

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
                                                        terminal_id, amount])
                
        self.dataset = pd.DataFrame(customer_transactions, columns=['TX_TIME_SECONDS', 'TX_TIME_DAYS', 'CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT'])
        
        if len(self.dataset)>0:
            self.dataset['TX_DATETIME'] = pd.to_datetime(self.dataset["TX_TIME_SECONDS"], unit='s', origin=start_date)
            self.dataset = self.dataset[['TX_DATETIME','CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT','TX_TIME_SECONDS', 'TX_TIME_DAYS']]
        
        self.gen_time = time.time()-self.gen_time
        
        return self.dataset