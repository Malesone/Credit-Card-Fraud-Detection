from statistics import Statistic
from datasets import Dataset
import time
from neo4j_app import App
from datasets import Operation

if __name__ == "__main__":
    d = Dataset()
    #d.generate_dataset(n_customers = 5700, n_terminals = 10000, nb_days = 150, radius = 10) 
    d.read_dataset() 
    
    load = Statistic(type = Operation.generation.value)
    app = App()
    app.delete_all()
    
    app.create_all(d.customers.dataset, d.terminals.dataset, d.transactions.dataset)
    load.stop_time()
    
    """extension = Statistic(type = Operation.extension.value)
    app.extension()
    extension.stop_time()
    
    tpp = Statistic(type = Operation.tpp.value)
    app.transactions_per_period()
    tpp.stop_time()
    """
    app.close()

    #d.statistics.extend([load, extension, tpp])
    d.statistics.append(load)
    d.gen_plot()
