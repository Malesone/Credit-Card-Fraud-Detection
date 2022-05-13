from pandas import DataFrame
from gen_customers import generate_customer_profiles_table
from gen_terminals import generate_terminal_profiles_table, get_list_terminals_within_radius
from gen_transactions import generate_transactions_table
import time
import random

class Dataset: 
    customer_profiles_table: DataFrame
    terminal_profiles_table: DataFrame
    transactions_df: DataFrame

    def __init__(self, n_customers, n_terminals, nb_days, start_date, r): 
        self.generate_dataset(n_customers, n_terminals, nb_days, start_date, r)
        
    def generate_dataset(self, n_customers, n_terminals, nb_days, start_date, r): # n_customers = 10000, n_terminals = 1000000, nb_days=90, start_date="2018-04-01", r=5):
        start_time=time.time()
        customer_profiles_table = generate_customer_profiles_table(n_customers, random_state = 0)
        print("Time to generate customer profiles table: {0:.2}s".format(time.time()-start_time))
        
        start_time=time.time()
        terminal_profiles_table = generate_terminal_profiles_table(n_terminals, random_state = 1)
        print("Time to generate terminal profiles table: {0:.2}s".format(time.time()-start_time))
        
        start_time=time.time()
        x_y_terminals = terminal_profiles_table[['x_terminal_id','y_terminal_id']].values.astype(float)
        customer_profiles_table['available_terminals'] = customer_profiles_table.apply(lambda x : get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, r=r), axis=1)
        # With Pandarallel
        #customer_profiles_table['available_terminals'] = customer_profiles_table.parallel_apply(lambda x : get_list_closest_terminals(x, x_y_terminals=x_y_terminals, r=r), axis=1)
        customer_profiles_table['nb_terminals']=customer_profiles_table.available_terminals.apply(len)
        print("Time to associate terminals to customers: {0:.2}s".format(time.time()-start_time))
        
        start_time=time.time()
        transactions_df=customer_profiles_table.groupby('CUSTOMER_ID').apply(lambda x : generate_transactions_table(x.iloc[0], nb_days=nb_days)).reset_index(drop=True)
        # With Pandarallel
        #transactions_df=customer_profiles_table.groupby('CUSTOMER_ID').parallel_apply(lambda x : generate_transactions_table(x.iloc[0], nb_days=nb_days)).reset_index(drop=True)
        print("Time to generate transactions: {0:.2}s".format(time.time()-start_time))
        
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

        self.transactions_df = transactions_df    

d = Dataset(n_customers = 10, n_terminals = 10, nb_days=10, start_date="2022-01-01", r=5)
print(d.customer_profiles_table)
print(d.terminal_profiles_table)
print(d.transactions_df)
d.add_frauds(d.customer_profiles_table, d.terminal_profiles_table, d.transactions_df)
print(d.transactions_df)