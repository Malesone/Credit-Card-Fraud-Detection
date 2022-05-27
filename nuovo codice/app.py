from datasets import Dataset
import time

if __name__ == "__main__":
    d = Dataset()
    d.generate_dataset(100000, 20000) 
    d.customers.get_dataset()
    d.terminals.get_dataset()
    
    d.to_pickle()
    d.deserializate()
    
    #print("total gen CUSTOMERS: ", d.customers.gen_time)
    #print("total gen TERMINALS: ", d.terminals.gen_time)
    print("total time: ", d.total_time)
    