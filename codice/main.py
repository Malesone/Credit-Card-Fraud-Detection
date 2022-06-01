from statistics import Statistic
from datasets import Dataset
import time
from app import App
from datasets import Operation

if __name__ == "__main__":
    start_time=time.time()
    d = Dataset()
    d.generate_dataset(n_customers = 10, n_terminals = 10, nb_days = 10, radius = 5) 

    load = Statistic(type = Operation.generation.value)
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "test"
    app = App(uri, user, password)
    print("Connected")
    app.delete_all()
    print("old db deleted")
    app.create_all(d.customers.dataset, d.terminals.dataset, d.transactions.dataset)
    app.close()
    load.stop_time()
    d.statistics.append(load)
    
    for stat in d.statistics:
        print(stat.get_string())