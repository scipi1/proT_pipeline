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


def match_target(dataset_id)->None:
    """Matches input and target
    It reads the short input (process per batch) and extends it for each target sample (coupon, IST,...)

    Args:
        dataset_id (str), working dataset folder
    """
    
    #dataset_id = "dyconex_test"

    # define directories
    ROOT_DIR = ROOT_DIR = dirname(dirname(abspath(__file__)))
    sys.path.append(ROOT_DIR)
    INPUT_DIR,OUTPUT_DIR,INTERMEDIATE_DIR,CONTROL_DIR = get_dirs(ROOT_DIR, dataset_id)
    filepath_selected = join(CONTROL_DIR, selected_filename)
    filepath_lookup = join(CONTROL_DIR, lookup_filename)
    filepath_target = join(CONTROL_DIR, target_filename)


    df_input_short = pd.read_parquet(join(OUTPUT_DIR,trans_df_input_short)).set_index(trans_batch_label)
    df_trg = pd.read_csv(filepath_target, sep=target_sep)
    df_trg = df_trg[df_trg[target_batch_label].isin(df_input_short.index)] # filter out unavailable batches


    df_query = df_trg[[target_id_label, target_batch_label]].drop_duplicates().set_index(target_id_label)


    df_list = []  # temporary holder for all sub-DataFrames

    for idx in tqdm(df_query.index):
        batch = df_query.loc[idx, target_batch_label]

        # Copy the slice once
        df_temp = df_input_short.loc[batch].reset_index().copy()
        df_temp[trans_id_label] = idx

        # Append the sub-DataFrame to the list
        df_list.append(df_temp)

    # After the loop, concatenate everything in one go
    df_input = pd.concat(df_list, ignore_index=True)
    
    # Export
    df_input.to_parquet(join(OUTPUT_DIR, trans_df_input))
    
if __name__ == "__main__":
    match_target(dataset_id = "dyconex_test")