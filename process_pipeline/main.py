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

#from os import abspath
from os.path import dirname, join, abspath
import sys


def main(args):
    
    ROOT_DIR = ROOT_DIR = dirname(dirname(abspath(__file__)))
    sys.path.append(ROOT_DIR)
    
    INPUT_DIR,OUTPUT_DIR,INTERMEDIATE_DIR = get_dirs(ROOT_DIR)
    print("Root directory: ",ROOT_DIR)
    
    filepath_selected = join(INTERMEDIATE_DIR, selected_filename)
    filepath_lookup = join(INTERMEDIATE_DIR, lookup_filename)
    filepath_target = join(INTERMEDIATE_DIR, target_filename)
    
    # create lookup table
    if args.makelookup:
        generate_lookup(filepath_lookup)
        print("The new lookup has been generated!")
        sys.exit(0)
        
    #load processes
    _, processes = get_processes(INPUT_DIR,filepath_selected)
        
    # read Y (IST) file
    df_ist = pd.read_csv(filepath_target, sep=target_sep)
    
    
    #_____________________________ FOR DEBUGGING ______________________________________
    if args.devrun:
        test_id = df_ist[target_id_label].unique()[:500]
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
        df_pc = pd.read_csv(join(INTERMEDIATE_DIR,input_raw_filename), sep=",")
        df_book_mis = pd.read_csv(join(INTERMEDIATE_DIR, booking_missing_filename), sep=",")
        df_ist_tr = pd.read_csv(join(INTERMEDIATE_DIR, target_trimmed_filename), sep=",")
    else:
        print("Generating the process chain...")
        # get the booking file
        df_book = get_booking(INPUT_DIR)
        
        # get process sequence
        df_pc, df_book_mis,df_pro_mis = sequence_builder(
            df_key=df_book.copy(), 
            df_query=df_ist.copy(), 
            query_branches=(target_design_label,target_version_label,target_batch_label,target_id_label),
            processes=processes,
            filepath_selected = filepath_selected)
        
        df_book_mis.to_csv(join(INTERMEDIATE_DIR, booking_missing_filename), sep=standard_sep)
        df_pro_mis.to_csv(join(INTERMEDIATE_DIR,process_missing_filename), sep=standard_sep)
        df_pc.to_csv(join(INTERMEDIATE_DIR,input_raw_filename), sep=standard_sep)
        
        # check dimensions and trim
        df_ist_tr = data_trimmer(df_x=df_pc.copy(), df_y=df_ist.copy(), df_miss=df_book_mis.copy(), 
                                save_path=INTERMEDIATE_DIR, save_file=True)   
        
    # check that ids are aligned
    df_pc_sort = df_pc.sort_values(target_id_label)
    df_ist_tr_sort = df_ist_tr.sort_values(target_id_label)
    
    if all(df_pc_sort[target_id_label].drop_duplicates().to_numpy() == df_ist_tr_sort[target_id_label].drop_duplicates().to_numpy()):
        print("All IDs are aligned! Proceed conversion to numpy arrays")
        
        # get absolute positions and missing values
        df_lev,max_seq_len_x,_ = level_sequences(df=df_pc,processes=processes,save_dir_templates=OUTPUT_DIR)
        
        df_lev.to_csv(join(INTERMEDIATE_DIR,input_leveled_filename))
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
    
    parser.add_argument("--devrun",
                        action="store_true",
                        help='Run a quick test for debugging purpose')
    
    parser.add_argument("--makelookup", 
                        type=bool,
                        choices=[True,False],
                        default=False,
                        help="generate new lookup table and exit")
    
    parser.add_argument("--readfile", 
                        action="store_true",
                        default=False,
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
        