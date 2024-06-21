import pandas as pd
import os
from pathlib import Path
from os.path import dirname, abspath,join
import sys
sys.path.append(dirname(dirname(abspath(__file__))))
from process_pipeline.core.modules import get_data_step
from process_pipeline.data_loader import get_processes, get_booking
from process_pipeline.utils import nested_dict_from_pandas

#root = dirname(os.getcwd())
root =Path(os.getcwd()).parent
#format = "%m/%d/%y %I:%M %p"
input_data_path = join(root,"data/input/")
intermediate_data_path = join(root,"data/intermediate/")
output_data_path = join(root,"data/output/")

filename_sel  = intermediate_data_path +"lookup_selected.xlsx"
filename_look = intermediate_data_path +"lookup.xlsx"

#load booking
df_book = get_booking(input_data_path)

#load processes
_, processes = get_processes(input_data_path,filename_sel)

# read Y (IST) file
df_ist = pd.read_csv(intermediate_data_path + "y_ist.csv", sep=",")
df_ist = df_ist.iloc[:100000]

# create a nested dictionary from Y (IST) queries
d = nested_dict_from_pandas(df_ist.set_index(["SapNummer","Version","WA","id"]))

# query the following coordinates
sap = 426816
ver = 'C'
wa = 'CTBV'

# select from booking dataframe
df_sel = df_book.set_index(["SAP","SAP_Version","WA"]).loc[sap].loc[ver].loc[wa]
steps = df_sel["PaPosNumber"]

df_list = None
for step in steps:
    df,_ = get_data_step(wa,step,processes,filename_sel)
    if df_list is None:
        df_list = df
    else:
        df_list = pd.concat([df_list,df])

found_processes = df_list["Process"].unique()

print(f"DataFrame assembled for the following coordinates SAP={sap}, version={ver}, WA={wa}")
print(f"The following processes where found {found_processes}")
