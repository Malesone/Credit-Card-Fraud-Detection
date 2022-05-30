from datasets import Dataset
import time

if __name__ == "__main__":
    d = Dataset()
    d.generate_dataset(n_customers = 2500, n_terminals = 20, nb_days = 5, radius = 10) 
    #d.customers.get_dataset()
    #d.terminals.get_dataset()
    
    d.to_pickle()
    d.deserializate()
    
    #for stat in d.statistics:
    #    print(stat.get_string())
    