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
from tqdm import tqdm
from core.modules import explode_time_components



def process_raw(dataset_id)->None:
    """Process raw process data

    Args:
        dataset_id (str): working directory
    """
    
    # define directories
    ROOT_DIR = ROOT_DIR = dirname(dirname(abspath(__file__)))
    sys.path.append(ROOT_DIR)
    INPUT_DIR,OUTPUT_DIR,INTERMEDIATE_DIR,CONTROL_DIR = get_dirs(ROOT_DIR, dataset_id)


    df_raw = pd.read_csv(join(OUTPUT_DIR,trans_df_process_raw))

    # normalization function(s)
    def max_normalizer(df:pd.DataFrame,var_label, val_label):
        max_map = df.groupby(var_label)[val_label].max()
        df[trans_value_norm_label] = df[val_label]/df[var_label].map(max_map) 
        return df
    
    
    # take mean of multiple variable measurements
    grouping_cols = [trans_batch_label, trans_position_label, trans_process_label, trans_variable_label]
    df_input_short = df_raw.groupby(grouping_cols).agg({
        trans_value_label: "mean",
        trans_date_label: "first",
        trans_parameter_label: "first"
        }).reset_index()
    
    
    #normalize
    df_input_short = max_normalizer(df_input_short, var_label=trans_variable_label, val_label=trans_value_label)
    
    #explode time
    df_input_short,_ = explode_time_components(df_input_short,trans_date_label)
    
    # export
    df_input_short.to_parquet(join(OUTPUT_DIR, trans_df_input_short))
    
    
if __name__ == "__main__":
    process_raw(dataset_id = "dyconex_test")