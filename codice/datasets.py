from lib2to3.pgen2.token import OP
import textwrap
from typing import Any
from neo4j import Transaction
from pandas import DataFrame
from customers import Customer
from terminals import Terminal
from transactions import Transaction
from statistics import Statistic

from matplotlib import pyplot as plt

import datetime
import random
import os
import pickle
import pandas as pd
import csv
import numpy as np
import time 
from enum import Enum
import matplotlib.pyplot as plt

class Operation(Enum):
    customers = "generate customers"
    terminals = "generate terminals"
    transactions = "generate transactions"
    generation = "datasets generation"
    deserialization = "deserialize"
    save = "save"
    extension = "extension"
    tpp = "get transactions per period"

class Dataset: 
    customers: Customer
    terminals: Terminal
    transactions: Transaction
    statistics = []

    DIR_PKL = "./dataset_200MB/"
    DIR_CSV = "./dataset_200MB/"

    def generate_dataset(self, n_customers, n_terminals, nb_days, radius):
        gen = Statistic(type = Operation.generation.value)
        self.customers = Customer(True, n_customers)
        
        self.terminals = Terminal(True, n_terminals)
        
        self.transactions = Transaction()
        self.gen_transaction(nb_days, radius)
        self.transactions.dataset = self.add_frauds(self.customers.dataset, self.terminals.dataset, self.transactions.dataset)
        
        self.calculate_amounts()
        gen.stop_time()
        self.statistics.append(gen)

        self.save_all()

    def read_dataset(self):
        self.customers = Customer(False, path=self.DIR_PKL+"customers.pkl")
        self.terminals = Terminal(False, path=self.DIR_PKL+"terminals.pkl")
        self.transactions = Transaction()
        self.transactions.dataset = pd.read_pickle(self.DIR_PKL+"transactions.pkl")
        
        print("dataset readed")
        #self.to_csv()
        self.deserializate()

    def save_all(self):
        self.to_pickle()
        self.deserializate()
        
    def add_frauds(self, customer_profiles_table, terminal_profiles_table, transactions_df):
        # By default, all transactions are genuine
        transactions_df['TX_FRAUD']=0
        transactions_df['TX_FRAUD_SCENARIO']=0
        
        # Scenario 1
        transactions_df.loc[transactions_df.TX_AMOUNT>220, 'TX_FRAUD']=1
        transactions_df.loc[transactions_df.TX_AMOUNT>220, 'TX_FRAUD_SCENARIO']=1
        nb_frauds_scenario_1=transactions_df.TX_FRAUD.sum()
        print("Number of frauds from scenario 1: "+str(nb_frauds_scenario_1))
        
        # Scenario 2
        for day in range(transactions_df.TX_TIME_DAYS.max()):
            
            compromised_terminals = terminal_profiles_table.TERMINAL_ID.sample(n=2, random_state=day)
            
            compromised_transactions=transactions_df[(transactions_df.TX_TIME_DAYS>=day) & 
                                                        (transactions_df.TX_TIME_DAYS<day+28) & 
                                                        (transactions_df.TERMINAL_ID.isin(compromised_terminals))]
                                
            transactions_df.loc[compromised_transactions.index,'TX_FRAUD']=1
            transactions_df.loc[compromised_transactions.index,'TX_FRAUD_SCENARIO']=2
        
        nb_frauds_scenario_2=transactions_df.TX_FRAUD.sum()-nb_frauds_scenario_1
        print("Number of frauds from scenario 2: "+str(nb_frauds_scenario_2))
        
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
        print("Number of frauds from scenario 3: "+str(nb_frauds_scenario_3))
        
        return transactions_df                 

    def get_stats(self):
        #Number of transactions per day
        nb_tx_per_day=self.transactions_df.groupby(['TX_TIME_DAYS'])['CUSTOMER_ID'].count()
        #Number of fraudulent transactions per day
        nb_fraud_per_day=self.transactions_df.groupby(['TX_TIME_DAYS'])['TX_FRAUD'].sum()
        #Number of fraudulent cards per day
        nb_fraudcard_per_day=self.transactions_df[self.transactions_df['TX_FRAUD']>0].groupby(['TX_TIME_DAYS']).CUSTOMER_ID.nunique()
        
        return (nb_tx_per_day,nb_fraud_per_day,nb_fraudcard_per_day)

    def gen_transaction(self, nb_days, radius):
        #prendo la coppia di valori delle coordinate
        x_y_terminals = self.terminals.dataset[['x_terminal_id','y_terminal_id']].values.astype(float)
        
        #per ciascun customer setto i terminali disponibili
        self.customers.dataset['available_terminals']=self.customers.dataset.apply(lambda x : self.get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, r=radius), axis=1)
        

        #setta quanti terminali sono disponibili per cliente
        self.customers.dataset['nb_terminals']=self.customers.dataset.available_terminals.apply(len)

        transactions_df=self.customers.dataset.groupby('CUSTOMER_ID').apply(lambda x : self.transactions.generate_transactions_table(x.iloc[0], nb_days=nb_days)).reset_index(drop=True)

        # Sort transactions chronologically
        transactions_df=transactions_df.sort_values('TX_DATETIME')
        # Reset indices, starting from 0
        transactions_df.reset_index(inplace=True,drop=True)
        transactions_df.reset_index(inplace=True)
        # TRANSACTION_ID are the dataframe indices, starting from 0
        transactions_df.rename(columns = {'index':'TRANSACTION_ID'}, inplace = True)

        self.transactions.dataset = transactions_df
       
    def get_list_terminals_within_radius(self, customer_profile, x_y_terminals, r):
        x_y_customer = customer_profile[['x_customer_id','y_customer_id']].values.astype(float)
        squared_diff_x_y = np.square(x_y_customer - x_y_terminals)
        dist_x_y = np.sqrt(np.sum(squared_diff_x_y, axis=1))
        available_terminals = list(np.where(dist_x_y<r)[0])
        return available_terminals

    def to_pickle(self): #salva tutti i dati generati sotto forma di .pkl
        save = Statistic(type = Operation.save.value)
        if not os.path.exists(self.DIR_PKL):
            os.makedirs(self.DIR_PKL)
        
        self.customers.dataset.to_pickle(self.DIR_PKL+"customers.pkl", protocol=4)
        
        self.terminals.dataset.to_pickle(self.DIR_PKL+"terminals.pkl", protocol=4)

        self.transactions.dataset.to_pickle(self.DIR_PKL+"transactions.pkl", protocol=4)
        
        save.stop_time()
        self.statistics.append(save)

    def deserializate(self):
        stat = Statistic(Operation.deserialization.value) 
        files = []
        total_size = 0
        if not os.path.exists(self.DIR_CSV):
                os.makedirs(self.DIR_CSV)

        cust = self.DIR_CSV+"customers.csv"
        terms = self.DIR_CSV+"terminals.csv"
        trans = self.DIR_CSV+"transactions.csv"

        self.customers.dataset.to_csv(cust, index=False)
        self.terminals.dataset.to_csv(terms, index=False)
        self.transactions.dataset.to_csv(trans, index=False)
        
        total_size = os.path.getsize(cust) +  os.path.getsize(terms) + os.path.getsize(trans) 
        total_size = total_size / 1024 / 1024
        print("Total dim: ", total_size, "MB")
        
        stat.stop_time()
        self.statistics.append(stat)

    def calculate_amounts(self):
        sum = []
        for id in self.customers.dataset['CUSTOMER_ID']:
            sum.append(self.transactions.dataset[self.transactions.dataset['CUSTOMER_ID']==id]['TX_AMOUNT'].sum())
            
        self.customers.dataset['AMOUNT'] = sum

    def gen_plot(self):
        statOps = []
        statVal = []
        text = ""
        for stat in self.statistics: 
            text += r' '+stat.get_string() + "\n"
            statOps.append(stat.type)
            statVal.append(stat.time)

        fig = plt.figure()
        ax1 = fig.add_axes((0.1, 0.2, 0.8, 0.7))
        
        plt.bar(statOps, statVal, align="center")
        ax1.set_title("Operations")
        ax1.set_xlabel('Operation')
        ax1.set_ylabel('Time')
        
        fig.text(.5, .05, text, ha='center', bbox={"facecolor": "orange", "alpha": 0.5})
        fig.set_size_inches(7, 8, forward=True)

        plt.show()
