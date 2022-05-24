from typing import Any
from pandas import DataFrame
from gen_customers import generate_customer_profiles_table
from gen_terminals import generate_terminal_profiles_table, get_list_terminals_within_radius
from gen_transactions import generate_transactions_table

import time
import datetime
import random
import os
import pickle
import pandas as pd
import csv

def save():
    DIR_OUTPUT = "./transactions/"

    if not os.path.exists(DIR_OUTPUT):
        os.makedirs(DIR_OUTPUT)

    start_date = datetime.datetime.strptime("2022-01-01", "%Y-%m-%d")

    for day in range(d.transactions_df.TX_TIME_DAYS.max()+1):
        
        transactions_day = d.transactions_df[d.transactions_df.TX_TIME_DAYS==day].sort_values('TX_TIME_SECONDS')
        
        date = start_date + datetime.timedelta(days=day)
        filename_output = date.strftime("%Y-%m-%d")+'.pkl'
        
        transactions_day.to_pickle(DIR_OUTPUT+filename_output, protocol=4)

def save_all():
    DIR_OUTPUT = "./pkl_all/"

    if not os.path.exists(DIR_OUTPUT):
        os.makedirs(DIR_OUTPUT)
    
    transactions = d.transactions_df
    transactions.to_pickle(DIR_OUTPUT+"transactions.pkl", protocol=4)
    
    customer_profiles_table = d.customer_profiles_table
    customer_profiles_table.to_pickle(DIR_OUTPUT+"customers.pkl", protocol=4)
    
    terminal_profiles_table = d.terminal_profiles_table
    terminal_profiles_table.to_pickle(DIR_OUTPUT+"terminals.pkl", protocol=4) 

def deserializate(): 
    folder = 'pkl_all/'
    files = []

    DIR_OUTPUT = "./csv-github/"

    if not os.path.exists(DIR_OUTPUT):
            os.makedirs(DIR_OUTPUT)

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

class Dataset: 
    customer_profiles_table: DataFrame
    terminal_profiles_table: DataFrame
    transactions_df: DataFrame
    x: Any
     
    def generate_dataset(self, n_customers, n_terminals, nb_days, start_date, r):
        start_time=time.time()
        customer_profiles_table = generate_customer_profiles_table(n_customers, random_state = 0)
        #print("Time to generate customer profiles table: {0:.2}s".format(time.time()-start_time))
        
        start_time=time.time()
        terminal_profiles_table = generate_terminal_profiles_table(n_terminals, random_state = 1)
        #print("Time to generate terminal profiles table: {0:.2}s".format(time.time()-start_time))
        
        start_time=time.time()
        x_y_terminals = terminal_profiles_table[['x_terminal_id','y_terminal_id']].values.astype(float)
        customer_profiles_table['available_terminals'] = customer_profiles_table.apply(lambda x : get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, r=r), axis=1)
        # With Pandarallel
        #customer_profiles_table['available_terminals'] = customer_profiles_table.parallel_apply(lambda x : get_list_closest_terminals(x, x_y_terminals=x_y_terminals, r=r), axis=1)
        customer_profiles_table['nb_terminals']=customer_profiles_table.available_terminals.apply(len)
        #print("Time to associate terminals to customers: {0:.2}s".format(time.time()-start_time))
        
        start_time=time.time()
        transactions_df=customer_profiles_table.groupby('CUSTOMER_ID').apply(lambda x : generate_transactions_table(x.iloc[0], nb_days=nb_days)).reset_index(drop=True)
        # With Pandarallel
        #transactions_df=customer_profiles_table.groupby('CUSTOMER_ID').parallel_apply(lambda x : generate_transactions_table(x.iloc[0], nb_days=nb_days)).reset_index(drop=True)
        #print("Time to generate transactions: {0:.2}s".format(time.time()-start_time))
        
        # Sort transactions chronologically
        transactions_df=transactions_df.sort_values('TX_DATETIME')
        # Reset indices, starting from 0
        transactions_df.reset_index(inplace=True,drop=True)
        transactions_df.reset_index(inplace=True)
        # TRANSACTION_ID are the dataframe indices, starting from 0
        transactions_df.rename(columns = {'index':'TRANSACTION_ID'}, inplace = True)

        self.customer_profiles_table = customer_profiles_table
        self.terminal_profiles_table = terminal_profiles_table
        self.transactions_df = transactions_df

        return (customer_profiles_table, terminal_profiles_table, transactions_df)
        
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

d = Dataset()
def generate_all():
    #dataset = d.generate_dataset(20, 50, 10, "2022-01-01", 18)
    dataset = d.generate_dataset(30, 60, 20, "2022-01-01", 15)
    save_all()
    deserializate()

def get_dataset():
    return (d.customer_profiles_table, d.terminal_profiles_table, d.transactions_df)