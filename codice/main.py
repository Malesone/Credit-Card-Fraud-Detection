from datasets import Dataset
import time
from app import App

if __name__ == "__main__":
    start_time=time.time()
    d = Dataset()
    d.generate_dataset(n_customers = 50, n_terminals = 50, nb_days = 10, radius = 5) 
    
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
    