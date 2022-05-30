from datasets import Dataset
import time
from app import App

if __name__ == "__main__":
    start_time=time.time()
    d = Dataset()
    #250, 500, 18, "2022-01-01", 5
    d.generate_dataset(n_customers = 2500, n_terminals = 5000, nb_days = 18, radius = 5) 
    tmp = time.time()-start_time
    print("Generazione: {0:.2}s".format(tmp))
    
    #d.to_pickle()
    #d.deserializate()
    start_time=time.time()
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "test"
    app = App(uri, user, password)
    print("Connected")
    app.delete_all()
    print("old db deleted")
    app.create_all(d.customers.dataset, d.terminals.dataset, d.transactions.dataset)
    
    app.close()
    tmp = time.time()-start_time
    print("Caricamento: {0:.2}s".format(tmp))
    
    #for stat in d.statistics:
    #    print(stat.get_string())
    