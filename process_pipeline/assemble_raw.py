import pandas as pd
import numpy as np
import sys
from data_loader import get_processes,get_booking,generate_lookup
from sequence_builder import sequence_builder
from data_trimmer import data_trimmer
from level_sequences import level_sequences
from data_post_processing import data_post_processing
from labels import *
from argparse import ArgumentParser
from fix_steps import fix_steps
import json
#from os import abspath
from os.path import dirname, join, abspath, exists
from os import makedirs
import sys



def assemble_raw(dataset_id,debug=False)->None:
    
    """
    Assembles a raw dataframe containing process data from the single
    process files, according to control files which select
    - which variables for each process to add
    - which step (PaPos) to include
    
    Args:
    dataset_id (str), working dataset folder 
    """
    
    dataset_id = "dyconex_test"

    # define directories
    ROOT_DIR = ROOT_DIR = dirname(dirname(abspath(__file__)))
    sys.path.append(ROOT_DIR)
    INPUT_DIR,OUTPUT_DIR,INTERMEDIATE_DIR,CONTROL_DIR = get_dirs(ROOT_DIR, dataset_id)
    filepath_selected = join(CONTROL_DIR, selected_filename)
    filepath_lookup = join(CONTROL_DIR, lookup_filename)
    filepath_target = join(CONTROL_DIR, target_filename)

    #load processes
    _, processes = get_processes(INPUT_DIR,filepath_selected)

    # read target (IST) file
    df_trg = pd.read_csv(filepath_target, sep=target_sep)
    query_batches = df_trg[input_batch_label].unique()
    if debug:
        query_batches = df_trg[input_batch_label].unique()[:20]
        

    # import control file for process selection
    df_steps_sel = pd.read_excel(join(CONTROL_DIR,selected_process_filename))
    steps_sel = np.array(df_steps_sel[df_steps_sel['Select']]["Step"])

    missing_batches_dic = {}
    df_raw = None

    
    df_list = []
    for pro in processes:

        # import control file (lookup table) for current process
        df_lookup = pd.read_excel(filepath_selected,sheet_name=pro.process_label)
        date_labels = [i for i in df_lookup["index"] if i in pro.date_label]        # date labels
        parameters = df_lookup[df_lookup["Select"]]["index"].tolist()              # selected parameters
        variables = df_lookup[df_lookup["Select"]][trans_variable_label].tolist()  # and relative variables

        assert len(parameters)==len(variables)                                     # create a dict with parameters (key) and variable name (values)
        params_vars = {parameters[i]:variables[i] for i in range(len(parameters))}

        df_cp = pro.df
        
        # address mismatch between query and key batches
        keys_batches = df_cp[pro.WA_label].unique()
        missing_batches = [query_batch for query_batch in query_batches if query_batch not in keys_batches]
        missing_batches_dic[pro.process_label] = missing_batches
        available_batches = [b for b in query_batches if b not in missing_batches]

        df_cp = df_cp.set_index(pro.WA_label).loc[available_batches].reset_index() # select available batches 
        
        # fix datetime
        datetime_list = [pd.to_datetime(df_cp[date_labels[0]],format=pro.date_format) for i in date_labels]
        
        if len(datetime_list)>1:
            print(f"Process {pro.process_label} has more than one date label...taking the mean")
                    
        df_cp[trans_date_label] = datetime_list[0]
        
        # melt dataframe
        df_cp = df_cp.melt(
            id_vars=[pro.WA_label,pro.PaPos_label,trans_date_label],
            value_vars=parameters,
            var_name=trans_parameter_label,
            value_name=trans_value_label)
        
        # add variables and process label
        df_cp[trans_variable_label] = df_cp[trans_parameter_label].map(params_vars)
        df_cp[trans_process_label] = pro.process_label
        
        # rename transversal columns
        df_cp = df_cp.rename(columns={
            pro.WA_label: trans_batch_label,
            pro.PaPos_label: trans_position_label,
            })
        df_list.append(df_cp)
    
    df_raw = pd.concat(df_list,ignore_index=True)
    
    # select steps from control file
    df_raw = df_raw[df_raw[trans_position_label].isin(steps_sel)]
    
    # check uniqueness of map position --> process
    df_unique_pairs = df_raw[[trans_position_label,trans_process_label]].drop_duplicates().sort_values(by=trans_position_label)
    count_process_per_position = df_unique_pairs.groupby(trans_position_label)[trans_process_label].nunique()
    df_check = count_process_per_position[count_process_per_position > 1]
    assert len(df_check)==0, AssertionError("Action needed! Some position ID is used for > 1 process")
    
    # export 
    df_raw.to_csv(join(OUTPUT_DIR, trans_df_process_raw))
    with open(join(OUTPUT_DIR, trans_missing_batches), "w") as f:
        json.dump(missing_batches_dic, f, indent=4)

if __name__ == "__main__":
    assemble_raw(dataset_id = "dyconex_test")