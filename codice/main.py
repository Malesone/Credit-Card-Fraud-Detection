import statistics
from datasets import Dataset
import time
from app import App
from datasets import Operation
from statistics import Statistic

if __name__ == "__main__":
    d = Dataset()
    d.generate_dataset(n_customers = 50, n_terminals = 50, nb_days = 10, radius = 5) 

    #upload = Statistic(type = Operation.terminals.value)
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "test"
    app = App(uri, user, password)
    print("Connected")
    app.delete_all()
    print("old db deleted")
    app.create_all(d.customers.dataset, d.terminals.dataset, d.transactions.dataset)
    
    app.close()
    
    #upload.stop_time()
    for stat in d.statistics:
        print(stat.get_string())
    
    