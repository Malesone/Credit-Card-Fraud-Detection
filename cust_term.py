#%%
from generation import Generation

gen = Generation(5, 5)

def get_plot_available_terminals(): 
    #print(gen.customers)
    gen.draw()
    gen.set_available_terminals()
    #print(gen.customers)

get_plot_available_terminals()



# %%
