from typing import List
# from os.path import dirname, abspath, join
# import sys
# sys.path.append((dirname(abspath(__file__))))


from os import getcwd
from os.path import dirname, join,abspath
import sys
ROOT_DIR = dirname(dirname(abspath(__file__)))
sys.path.append(ROOT_DIR)

from process_pipeline.core.modules import Process, get_df_lookup
from process_pipeline.utils import fix_format_columns
import pandas as pd


laser = Process(
    process_label = "Laser",
    hidden_label = "Process_1",
    machine_label = "Machine",
    WA_label = "WA",
    PaPos_label ="PaPosNr",
    date_label = ["TimeStamp","CreateDate 1"],
    date_format ="%m/%d/%y %I:%M %p",
    prefix ="las",
    filename = "laser.csv",
    sep = ";",
    header = 5
)

plasma = Process(
    process_label = "Plasma",
    hidden_label = "Process_2",
    machine_label = "Machine",
    WA_label = "WA",
    PaPos_label ="Position",
    date_label = ["Buchungsdatum",],
    date_format = "%m/%d/%y %I:%M %p",
    prefix ="pla",
    filename = "plasma.csv",
    sep = ";",
    header = 11
)

galvanic = Process(
    process_label = "Galvanic",
    hidden_label = "Process_3",
    machine_label = None,
    WA_label = "WA",
    PaPos_label = "PaPosNr",
    date_label = ["Date/Time Stamp"],
    date_format = "%m/%d/%y %I:%M %p",
    prefix = "gal",
    filename = "galvanik.csv",
    sep = ";",
    header = 11
)


multibond = Process(
    process_label = "Multibond",
    hidden_label = "Process_4",
    machine_label = None,
    WA_label = "WA",
    PaPos_label ="PaPosNr",
    date_label = ["t_StartDateTime"],
    date_format ="%m/%d/%y %I:%M %p",
    prefix ="mul",
    filename = "multibond.csv",
    sep = ";",
    header = 2
)


microetch = Process(
    process_label = "Microetch",
    hidden_label = "Process_5",
    machine_label = None,
    WA_label = "WA",
    PaPos_label ="PaPosNr",
    date_label = ["CreateDate"],
    date_format = "%d.%m.%Y %H:%M:%S",
    prefix ="mic",
    filename = "microetch.csv",
    sep = ";",
    header = 2
)


processes = [laser, plasma, galvanic, multibond, microetch]
processes_list = ["laser","plasma","galvanic","multibond","microetch"]


def generate_lookup(filename_look):  
    with pd.ExcelWriter(filename_look) as writer:
        for process in processes:
            get_df_lookup(process).to_excel(writer, sheet_name=process.process_label)

def get_processes(input_data_path,filename_sel,processes:List[Process] = processes):
    for process in processes:
        process.get_df(input_data_path)
        process.get_variables_list(filename_sel)
        process.normalize_df(filename_sel)
        #process.convert_timestamp()
    
    return processes_list, processes

def get_booking(input_data_path):
    df_book = fix_format_columns(pd.read_csv(join(input_data_path,"booking.csv"),sep=";"))
    df_book["Timestamp"] = pd.to_datetime(df_book["Timestamp"],format = "%m/%d/%y %I:%M %p")
    
    return df_book