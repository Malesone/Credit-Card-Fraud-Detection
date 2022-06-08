from numpy import choose
from datasets import Dataset
from datasets import Operation
from neo4j_app import App
from statistics import Statistic
from typing import Any

class Manage: 
    show = True
    d = Dataset()
    app = App()
    gen = [False, False] #se uno dei due è True vuol dire che il dataset è stato generato o letot

    def options(self, op):
        if op == 0: self.exit()
        elif op == 1: self.generate()
        elif op == 2: self.read()
        elif op == 3: self.upload()
        elif op == 4: self.exec_queries()
        elif op == 5: self.domain_extension()
        elif op == 6: self.statistics()

    def generate(self): #1. generazione dataset
        self.gen[0] = True

        dim = input("Scegli la dimensione in MB:\n50\n100\n200\n\nDimensione: ")
        values = self.check_dim(int(dim), 0)
        self.d.generate_dataset(values[0], values[1], values[2], values[3]) 
        #self.d.generate_dataset(100, 100, 10, 10) 

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
            self.d.statistics.append(load)

    def exec_queries(self): #4. esecuzione query
        if not(self.app.created): 
            self.app.create_app()
            
        print("Esecuzione query... ")
        exec = Statistic(type = Operation.queries_execution)
        self.app.execute_queries("02")
        exec.stop_time()
        self.d.statistics.append(exec)

    def domain_extension(self): #5. estensione dominio
        if not(self.app.created): 
            self.app.create_app()

        print("Estensione dominio... ")
        extension = Statistic(type = Operation.extension.value)
        self.app.extension()
        extension.stop_time()
        
        tpp = Statistic(type = Operation.tpp.value)
        self.app.transactions_per_period()
        tpp.stop_time()

        self.d.statistics.extend([extension, tpp])
    
    def statistics(self):
        if self.check_gen():
            self.d.gen_plot()

    def exit(self):
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
