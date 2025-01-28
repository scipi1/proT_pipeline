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

#from os import abspath
from os.path import dirname, join, abspath, exists
from os import makedirs
import sys


def main(args):
    
    dataset_id = "dyconex_453828B_241031"
    select_design = 453828
    select_version = "B"
    
    ROOT_DIR = ROOT_DIR = dirname(dirname(abspath(__file__)))
    sys.path.append(ROOT_DIR)
    
    INPUT_DIR,OUTPUT_DIR,INTERMEDIATE_DIR,CONTROL_DIR = get_dirs(ROOT_DIR, dataset_id)
    filepath_selected = join(CONTROL_DIR, selected_filename)
    filepath_lookup = join(CONTROL_DIR, lookup_filename)
    filepath_target = join(CONTROL_DIR, target_filename)
    
    # create lookup table
    if args.makelookup:
        generate_lookup(filepath_lookup)
        print("The new lookup has been generated!")
        sys.exit(0)
        
    #load processes
    _, processes = get_processes(INPUT_DIR,filepath_selected)
        
    # read Y (IST) file
    df_ist = pd.read_csv(filepath_target, sep=target_sep)
    
    # eventually reduce/select IST queries
    if select_design is not None and select_version is not None:
        df_ist = df_ist.set_index([target_design_label,target_version_label]).loc[[(select_design,select_version)]].reset_index()
    
    
    #_____________________________ FOR DEBUGGING ______________________________________
    if args.debug:
        test_id = df_ist[target_id_label].unique()[:200]
        df_ist = df_ist.set_index(target_id_label).loc[test_id].reset_index()
        
    if args.target_design is not None:
        df_ist = df_ist[df_ist[target_design_label]==args.target_design].reset_index()
        print(f"Target Design mode: design {args.target_design} selected from target data")
        
    if args.target_id is not None:
        df_ist = df_ist[df_ist[target_id_label]==args.target_id].reset_index()
        print(f"Target ID mode: design {args.target_id} selected from target data")
    #___________________________________________________________________________________
    
    if args.readfile:
        print("Reading the process chain from file")
        df_pc = pd.read_csv(join(CONTROL_DIR,input_raw_filename), sep=",")
        df_book_mis = pd.read_csv(join(CONTROL_DIR, booking_missing_filename), sep=",")
        df_ist_tr = pd.read_csv(join(CONTROL_DIR, target_trimmed_filename), sep=",")
    else:
        print("Generating the process chain...")
        # get the booking file
        df_book = get_booking(INPUT_DIR)
        
        df_steps_sel = pd.read_excel(join(CONTROL_DIR,"steps_selected.xlsx"))
        steps_sel = np.array(df_steps_sel[df_steps_sel['Select']]["Step"])
        
        # get process sequence
        df_pc, df_book_mis,df_pro_mis = sequence_builder(
            df_key=df_book.copy(), 
            df_query=df_ist.copy(), 
            query_branches=(target_design_label,target_version_label,target_batch_label,target_id_label),
            processes=processes,
            selected_steps = steps_sel,
            filepath_selected = filepath_selected)
        
        # Fix non-uniques step-process pairs
        df_pc = fix_steps(df_pc)
        
        print(len(df_pc))
        # remove unwanted steps(wrap in a function)
        # df_steps_sel = pd.read_excel(join(INTERMEDIATE_DIR,"steps_selected.xlsx"))
        # steps = np.array(df_steps_sel[df_steps_sel['Select']]["Step"])
        steps_mask = df_pc[input_step_label].isin(steps_sel)
        # masked_out_id = df_pc[np.logical_not(steps_mask)][target_id_label].unique()
        df_pc = df_pc[steps_mask]
        # masked_in_id = df_pc[target_id_label].unique()
        # id_excluded = [id_ for id_ in masked_out_id if id_ not in masked_in_id]
        
        
        
        df_book_mis.to_csv(join(OUTPUT_DIR, booking_missing_filename), sep=standard_sep)
        df_pro_mis.to_csv(join(OUTPUT_DIR,process_missing_filename), sep=standard_sep)
        df_pc.to_csv(join(OUTPUT_DIR,input_raw_filename), sep=standard_sep)
        
        # check dimensions and trim
        df_ist_tr = data_trimmer(
            df_x=df_pc.copy(), 
            df_y=df_ist.copy(), 
            df_miss=df_book_mis.copy(),
            #cut_id = id_excluded,
            save_path=OUTPUT_DIR, save_file=True)
        
        
    df_pc.to_csv(join(OUTPUT_DIR, "x_prochain_lev_sel.csv"))
    
    # check that ids are aligned
    df_pc_sort = df_pc.sort_values(target_id_label)
    df_ist_tr_sort = df_ist_tr.sort_values(target_id_label)
    
    if np.array_equal(
        df_pc_sort[target_id_label].drop_duplicates().to_numpy(), 
        df_ist_tr_sort[target_id_label].drop_duplicates().to_numpy()):        
        
        print("All IDs are aligned! Proceed conversion to numpy arrays")
        
        # get absolute positions and missing values
        df_lev, max_seq_len_x, df_templates = level_sequences(df=df_pc,processes=processes,save_dir=OUTPUT_DIR)
        
        df_templates.to_csv(join(OUTPUT_DIR,templates_filename))
        df_lev.to_csv(join(OUTPUT_DIR,input_leveled_filename))
        print(f"Leveling done! Maximum sequence length = {max_seq_len_x}")
        
        # post processing and conversion to numpy
        X_np = data_post_processing(
            df=df_lev, 
            time_label=input_time_label,
            id_label=input_id_label,
            sort_label=input_step_label,
            features=[input_value_label,input_abs_pos_label],
            max_seq_len=max_seq_len_x,
            cluster=args.cluster)
        
        Y_np = data_post_processing(
            df=df_ist_tr, 
            time_label=target_time_label,
            id_label=target_id_label,
            sort_label=target_pos_label,
            features=[target_value_label,target_pos_label],
            max_seq_len=args.seqleny,
            cluster=args.cluster)
        
        print("Dataset files successfully generated.")
        print(f"X_np shape: {X_np.shape}, Y_np shape: {Y_np.shape}")
    
        # save numpy arrays
        with open(join(OUTPUT_DIR, "X_np.npy"), 'wb') as f:
            np.save(f, X_np)
    
        with open(join(OUTPUT_DIR, "Y_np.npy"), 'wb') as f:
            np.save(f, Y_np)
            
        print("Dataset files saved, end of the program")
        
    else:
        print("Problem: input and target are not aligned!")
        
        
if __name__ == '__main__':
    parser = ArgumentParser(
        prog='Process Pipeline',
        description='The program builds sequential datasets from processes and booking table',
        epilog='Text at the bottom of help')
    
    parser.add_argument("--target_design",
                        action="store_const",
                        const=426816,
                        help='Debugging a specific design')
    
    parser.add_argument("--target_id",
                        action="store",
                        help='Debugging a specific ID')
    
    parser.add_argument("--debug",
                        action="store_true",
                        help='Run a quick test for debugging purpose')
    
    parser.add_argument("--makelookup", 
                        action="store_true",
                        help="generate new lookup table and exit")
    
    parser.add_argument("--readfile", 
                        action="store_true",
                        help="reads existing files")
    
    parser.add_argument("--cluster", 
                        action="store_true",
                        help="running on cluster")
    
    parser.add_argument("--seqlenx", 
                        action="store",
                        default=1600,
                        help="maximum x sequence length")
    
    parser.add_argument("--seqleny", 
                        action="store",
                        default=250,
                        help="maximum y sequence length")
    
    args = parser.parse_args()
    
    main(args)
        