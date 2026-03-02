from typing import List
# from os.path import dirname, abspath, join
# import sys
# sys.path.append((dirname(abspath(__file__))))


from os import getcwd
from os.path import dirname, join,abspath
import sys
ROOT_DIR = dirname(dirname(abspath(__file__)))
sys.path.append(ROOT_DIR)
from omegaconf import OmegaConf

from proT_pipeline.core.modules import Process, get_df_lookup
from proT_pipeline.utils import fix_format_columns, safe_read_csv
import pandas as pd
from proT_pipeline.labels import *


laser = Process(
    process_label   = "Laser",
    hidden_label    = "Process_1",
    machine_label   = "Machine",
    WA_label        = "WA",
    panel_label     = "PanelNr",
    PaPos_label     = "PaPosNr",
    date_label      = ["TimeStamp","CreateDate 1"],
    date_format     = "%m/%d/%y %I:%M %p",
    prefix          = "las",
    filename        = "laser.csv",
    sep             = ";",
    header          = 5
)

plasma = Process(
    process_label   = "Plasma",
    hidden_label    = "Process_2",
    machine_label   = "Machine",
    WA_label        = "WA",
    panel_label     = "PanelNummer",
    PaPos_label     = "Position",
    date_label      = ["Buchungsdatum",],
    date_format     = "%m/%d/%y %I:%M %p",
    prefix          = "pla",
    filename        = "plasma.csv",
    sep             = ";",
    header          = 11
)

galvanic = Process(
    process_label   = "Galvanic",
    hidden_label    = "Process_3",
    machine_label   = None,
    WA_label        = "WA",
    panel_label     = "Panelnr",
    PaPos_label     = "PaPosNr",
    date_label      = ["Date/Time Stamp"],
    date_format     = "%m/%d/%y %I:%M %p",
    prefix          = "gal",
    filename        = "galvanik.csv",
    sep             = ";",
    header          = 11
)


multibond = Process(
    process_label   = "Multibond",
    hidden_label    = "Process_4",
    machine_label   = None,
    WA_label        = "WA",
    panel_label     = None,
    PaPos_label     ="PaPosNr",
    date_label      = ["t_StartDateTime"],
    date_format     ="%m/%d/%y %I:%M %p",
    prefix          ="mul",
    filename        = "multibond.csv",
    sep             = ";",
    header          = 2
)


microetch = Process(
    process_label   = "Microetch",
    hidden_label    = "Process_5",
    machine_label   = None,
    WA_label        = "WA",
    panel_label     = None,
    PaPos_label     = "PaPosNr",
    date_label      = ["CreateDate"],
    date_format     = "%d.%m.%Y %H:%M:%S",
    prefix          = "mic",
    filename        = "microetch.csv",
    sep             = ";",
    header          = 2
)


processes = [laser, plasma, galvanic, multibond, microetch]


def generate_lookup(filename_look):  
    with pd.ExcelWriter(filename_look) as writer:
        for process in processes:
            get_df_lookup(process).to_excel(writer, sheet_name=process.process_label)

def get_processes(input_data_path,filename_sel, grouping_method: str, grouping_column:str, processes:List[Process]=processes, selected_process_labels: List[str]=None):
    """
    Load and initialize process objects from data files.
    
    Args:
        input_data_path: Path to input data folder
        filename_sel: Path to lookup_selected.xlsx file
        grouping_method: Grouping method ("panel" or "column")
        grouping_column: Column name for grouping (if method is "column")
        processes: List of Process objects to use (default: all defined processes)
        selected_process_labels: Optional list of process labels to filter (e.g., ["Laser", "Plasma"]).
                                 If None, all processes are loaded.
    
    Returns:
        Tuple of (process_labels list, processes list)
    """
    process_map = None
    try:
        process_map = OmegaConf.load(join(input_data_path,"process_map.yaml"))
    except:
        print("process_map.yaml not found! Proceed with hard-coded options.")
    
    processes_list = []
    
    if process_map is not None:
        processes = []
        for key in process_map.keys():
            processes.append(
                Process(
                    process_label   = process_map[key]["process_label"],
                    hidden_label    = process_map[key]["hidden_label"],
                    machine_label   = process_map[key]["machine_label"],
                    WA_label        = process_map[key]["WA_label"],
                    panel_label     = process_map[key]["panel_label"],
                    PaPos_label     = process_map[key]["PaPos_label"],
                    date_label      = process_map[key]["date_label"],
                    date_format     = process_map[key]["date_format"],
                    prefix          = process_map[key]["prefix"],
                    filename        = process_map[key]["filename"],
                    sep             = process_map[key]["sep"],
                    header          = process_map[key]["header"]
                )
            )
    
    # Filter processes if specific labels are requested
    if selected_process_labels is not None:
        available_labels = [p.process_label for p in processes]
        processes = [p for p in processes if p.process_label in selected_process_labels]
        if len(processes) == 0:
            raise ValueError(
                f"No matching processes found for labels: {selected_process_labels}. "
                f"Available process labels: {available_labels}"
            )
        # Warn about any requested labels that weren't found
        not_found = [label for label in selected_process_labels if label not in available_labels]
        if not_found:
            print(f"Warning: Requested process labels not found and skipped: {not_found}")
            
    
    for process in processes:
        process.get_df(input_data_path)
        process.get_variables_list(filename_sel)
        process.normalize_df(filename_sel)
        process.df = get_group_id(
            process=process, 
            grouping_method=grouping_method, 
            grouping_column=grouping_column
            )

        processes_list.append(process.process_label)
        
        #process.convert_timestamp()
    
    return processes_list, processes

def get_booking(input_data_path):
    df_book = fix_format_columns(safe_read_csv(join(input_data_path,"booking.csv"),sep=";"))
    df_book["Timestamp"] = pd.to_datetime(df_book["Timestamp"],format = "%m/%d/%y %I:%M %p")
    
    return df_book


def get_group_id(process:Process, grouping_method: str, grouping_column: str):
    """
    Adds the grouping columns according to the `grouping_method` or `grouping_column` specified

    Args:
        process (Process object): _description_
        grouping_method (str): _description_
        grouping_column (str, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_
    """
    
    assert grouping_method in ["panel", "column"]
    
    df_ = process.df
    
    if grouping_method == "panel":
        if process.panel_label is not None:
            df_['numeric_part'] = df_[process.panel_label].astype(str).str.extract(r'(\d+)')[0].astype("Int64")
            df_[trans_group_id] = df_[process.WA_label] + '_' + df_['numeric_part'].astype(str)
        else:
            # some processes (like multibond and microetch) process all panels together and don't have a panel column
            df_[trans_group_id] = df_[process.WA_label] + '_*'
        
    elif grouping_method == "column" and grouping_column is not None:
        if grouping_column == "batch":
            df_[trans_group_id] = df_[process.WA_label]
        else:
            raise ValueError(f"grouping column {grouping_column} invalid!")
    
    return df_
