from enum import Enum
from tokenize import String
import time

class Statistic:
    type: String
    time: float
    
    def __init__(self, type):
        self.type = type
        self.time = time.time()
    
    def stop_time(self):
        self.time = time.time()-self.time

    def get_string(self):
        return "Time to " + type + ": " + time
