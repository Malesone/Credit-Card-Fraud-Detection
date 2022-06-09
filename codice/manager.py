from numpy import choose
from datasets import Dataset
from datasets import Operation
from neo4j_app import App
from statistics import Statistic
from typing import Any
import matplotlib.pyplot as plt

class Manager: 
    show = True
    d = Dataset()
    app = App()
    gen = [False, False] #se uno dei due è True vuol dire che il dataset è stato generato o letot
    statistics = []

    def options(self, op):
        if op == 0: self.exit()
        elif op == 1: self.generate()
        elif op == 2: self.read()
        elif op == 3: self.upload()
        elif op == 4: self.exec_queries()
        elif op == 5: self.domain_extension()
        elif op == 6: self.get_statistics()

    def generate(self): #1. generazione dataset
        self.gen[0] = True
        dim = input("Scegli la dimensione in MB:\n50\n100\n200\n\nDimensione: ")
        values = self.check_dim(int(dim), 0)
        #self.statistics.append(self.d.generate_dataset(values[0], values[1], values[2], values[3]))
        self.d.generate_dataset(100, 100, 10, 10) 

    def read(self): #2. lettura dataset
        self.gen[1] = True
        dim = input("Scegli la dimensione in MB:\n50\n100\n200\n\nDimensione: ")
        values = self.check_dim(int(dim), 1)
        self.d.read_dataset(values[4])

    def check_dim(self, dim, call):
        values = [3000, 5000, 150, 10, "./dataset_50MB/"]
        if dim == 100:
            values = [5700, 10000, 150, 10, "./dataset_100MB/"]
        elif dim == 200:
            values = [11400, 20000, 150, 10, "./dataset_200MB/"]
        elif dim != 50: 
            #se call vale 0, è perché è una generazione, 1 lettura
            self.gen[call] = False

        return values

    def upload(self): #3. caricamento su db
        if self.check_gen():
            print("Caricamento del dataset sul database... ")
            load = Statistic(type = Operation.generation.value)
            self.app.delete_all()
            self.app.create_all(self.d.customers.dataset, self.d.terminals.dataset, self.d.transactions.dataset)
            load.stop_time()

            self.statistics.append(load)

    def exec_queries(self): #4. esecuzione query
        if not(self.app.created): 
            self.app.create_app()
        
        print("Esecuzione query... ")
        s1 = Statistic(type = Operation.amount_spent.value)
        print("Stampa somma totale per customer... ")
        self.app.return_amount_customer("02")
        s1.stop_time()
        s2 = Statistic(type = Operation.identify_fraud.value)
        print("Stampa transazioni fraudolente... ")
        self.app.fraudolent_transactions()
        s2.stop_time()
        id_customer = input("\n\nScegli il numero del customer: ")
        k_customer = input("Scegli il grado: ")
        s3 = Statistic(type = Operation.co_customer.value)

        print("Stampa co-customer in corso...")

        self.app.return_cocustomer(int(id_customer), int(k_customer))
        s3.stop_time()
        
        self.statistics.extend([s1, s2, s3])

    def domain_extension(self): #5. estensione dominio
        if not(self.app.created): 
            self.app.create_app()

        print("Estensione dominio... ")
        e1 = Statistic(type = Operation.moments_products.value)    
        self.app.extend_transactions()
        e1.stop_time()
        e2 = Statistic(type = Operation.buying_friends.value)    
        self.app.buying_friends()
        e2.stop_time()
        e3 = Statistic(type = Operation.tpp.value)
        self.app.transactions_per_period()
        e3.stop_time()
        print("Estensione completata")
        self.statistics.extend([e1, e2, e3])
    
    def get_statistics(self): #6. ottieni statistiche
        if self.check_gen():
            self.gen_plot()
        
        for stat in self.statistics:
            print(stat.get_string())

    def exit(self): #0. chiusura
        self.show = False
        if self.gen[0] or self.gen[1]:
            self.app.close()
            self.d.gen_plot()
        print("Chiusura")

    def start(self):
        while (self.show):
            menu = (
                "Scegli:\n"
                "0. Esci\n"
                "1. Genera dataset\n"
                "2. Leggi dataset\n"
                "3. Carica dataset su db\n"
                "4. Esegui query\n"
                "5. Estendi dominio\n"
                "6. Statistiche\n"
                "N.B: prima di effettuare qualsiasi operazione, effettuare operazioni 1 o 2\n\nScegli: "
            )
            val = input(menu)
            self.options(int(val))
            print("\n")
        
    def check_gen(self): #controlla se il dataset è stato generato o letto
        if not(self.gen[0]) and not(self.gen[1]):
            print("Scegli prima 1 o 2")
        else: 
            if not(self.app.created): 
                self.app.create_app()
            
        return self.gen[0] or self.gen[1]

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

if __name__ == "__main__":
    manager = Manager()
    manager.start()
