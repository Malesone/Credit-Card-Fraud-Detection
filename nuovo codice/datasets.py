from typing import Any
from pandas import DataFrame
from customers import Customer
from terminals import Terminal

import datetime
import random
import os
import pickle
import pandas as pd
import csv
import numpy as np
import time 

class Dataset: 
    customers: Customer
    terminals: Terminal
    transactions: Customer

    DIR_PKL = "./dataset_pkl/"
    DIR_CSV = "./dataset_csv/"

    total_time: float

    customer_profiles_table: DataFrame
    terminal_profiles_table: DataFrame
    transactions_df: DataFrame
    x: Any

    def generate_dataset(self, n_customers, n_terminals):
        self.customers = Customer(n_customers)
        self.terminals = Terminal(n_terminals)

        self.total_time = self.customers.gen_time + self.terminals.gen_time

    def add_frauds(self, customer_profiles_table, terminal_profiles_table, transactions_df):
        # By default, all transactions are genuine
        transactions_df['TX_FRAUD']=0
        transactions_df['TX_FRAUD_SCENARIO']=0
        
        # Scenario 1
        transactions_df.loc[transactions_df.TX_AMOUNT>220, 'TX_FRAUD']=1
        transactions_df.loc[transactions_df.TX_AMOUNT>220, 'TX_FRAUD_SCENARIO']=1
        nb_frauds_scenario_1=transactions_df.TX_FRAUD.sum()
        #print("Number of frauds from scenario 1: "+str(nb_frauds_scenario_1))
        
        # Scenario 2
        for day in range(transactions_df.TX_TIME_DAYS.max()):
            
            compromised_terminals = terminal_profiles_table.TERMINAL_ID.sample(n=2, random_state=day)
            
            compromised_transactions=transactions_df[(transactions_df.TX_TIME_DAYS>=day) & 
                                                        (transactions_df.TX_TIME_DAYS<day+28) & 
                                                        (transactions_df.TERMINAL_ID.isin(compromised_terminals))]
                                
            transactions_df.loc[compromised_transactions.index,'TX_FRAUD']=1
            transactions_df.loc[compromised_transactions.index,'TX_FRAUD_SCENARIO']=2
        
        nb_frauds_scenario_2=transactions_df.TX_FRAUD.sum()-nb_frauds_scenario_1
        #print("Number of frauds from scenario 2: "+str(nb_frauds_scenario_2))
        
        # Scenario 3
        for day in range(transactions_df.TX_TIME_DAYS.max()):
            
            compromised_customers = customer_profiles_table.CUSTOMER_ID.sample(n=3, random_state=day).values
            
            compromised_transactions=transactions_df[(transactions_df.TX_TIME_DAYS>=day) & 
                                                        (transactions_df.TX_TIME_DAYS<day+14) & 
                                                        (transactions_df.CUSTOMER_ID.isin(compromised_customers))]
            
            nb_compromised_transactions=len(compromised_transactions)
            
            
            random.seed(day)
            index_fauds = random.sample(list(compromised_transactions.index.values),k=int(nb_compromised_transactions/3))
            
            transactions_df.loc[index_fauds,'TX_AMOUNT']=transactions_df.loc[index_fauds,'TX_AMOUNT']*5
            transactions_df.loc[index_fauds,'TX_FRAUD']=1
            transactions_df.loc[index_fauds,'TX_FRAUD_SCENARIO']=3
        

                             
        nb_frauds_scenario_3=transactions_df.TX_FRAUD.sum()-nb_frauds_scenario_2-nb_frauds_scenario_1
        #print("Number of frauds from scenario 3: "+str(nb_frauds_scenario_3))

        self.transactions_df = transactions_df    

    def get_stats(self):
        #Number of transactions per day
        nb_tx_per_day=self.transactions_df.groupby(['TX_TIME_DAYS'])['CUSTOMER_ID'].count()
        #Number of fraudulent transactions per day
        nb_fraud_per_day=self.transactions_df.groupby(['TX_TIME_DAYS'])['TX_FRAUD'].sum()
        #Number of fraudulent cards per day
        nb_fraudcard_per_day=self.transactions_df[self.transactions_df['TX_FRAUD']>0].groupby(['TX_TIME_DAYS']).CUSTOMER_ID.nunique()
        
        return (nb_tx_per_day,nb_fraud_per_day,nb_fraudcard_per_day)

    def to_pickle(self): #salva tutti i dati generati sotto forma di .pkl
        if not os.path.exists(self.DIR_PKL):
            os.makedirs(self.DIR_PKL)

        save_customers = time.time()
        self.customers.dataset.to_pickle(self.DIR_PKL+"customers.pkl", protocol=4)
        print("save CUSTOMER: {0:.2}s".format(time.time()-save_customers))   
        
        save_terminals = time.time()
        self.terminals.dataset.to_pickle(self.DIR_PKL+"terminal.pkl", protocol=4)
        print("save TERMINALS: {0:.2}s".format(time.time()-save_terminals))   
        self.total_time += (time.time() - save_customers) + (time.time() - save_terminals)

    def deserializate(self): 
        deserializate = time.time()
        files = []

        if not os.path.exists(self.DIR_CSV):
                os.makedirs(self.DIR_CSV)

        dir = os.path.join('.','dataset_pkl')

        with os.scandir(self.DIR_PKL) as entries:
            for entry in entries:
                name = entry.name
                files.append(name)

        files.sort()

        for entry in files: 
            name = self.DIR_PKL+entry
            with open(name, 'rb') as f:
                data = pickle.load(f)
                if not os.path.exists(dir):
                    os.mkdir(dir)    
                nome = self.DIR_CSV+entry.replace(".pkl", ".csv")
                df = pd.DataFrame(data)
                df.to_csv(nome, index=False)

        print("deserializate CUSTOMER: {0:.2}s".format(time.time()-deserializate))   
        self.total_time += (time.time()-deserializate)

    