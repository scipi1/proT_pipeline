import pandas as pd
import os
from os.path import dirname, abspath,join
from pathlib import Path
import sys
sys.path.append(dirname(dirname(abspath(__file__))))
from process_pipeline.data_loader import get_processes,get_booking
from process_pipeline.sequence_builder import sequence_builder


root =Path(os.getcwd()).parent
#format = "%m/%d/%y %I:%M %p"
input_data_path = join(root,"data/input/")
intermediate_data_path = join(root,"data/intermediate/")
output_data_path = join(root,"data/output/")

filename_sel  = intermediate_data_path +"lookup_selected.xlsx"
filename_look = intermediate_data_path +"lookup.xlsx"


# load processes
_, processes = get_processes(input_data_path,filename_sel)

# read the keys file (Y target: IST)
df_ist = pd.read_csv(intermediate_data_path + "y_ist.csv", sep=",")
df_ist = df_ist.iloc[:100000]

# get the booking file
df_book = get_booking(input_data_path)

df_pc, df_book_mis,df_pro_mis = sequence_builder(df_query=df_book.copy(), df_keys=df_ist.copy(), keys_branches=["SapNummer","Version","WA","id"],
                        processes=processes, saving_path = intermediate_data_path, filename_sel=filename_sel)

print("All test passed!")