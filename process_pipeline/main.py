import pandas as pd
import numpy as np
import sys
from os.path import join
from config import get_folders
from data_loader import get_processes,get_booking,generate_lookup
from sequence_builder import sequence_builder
from data_trimmer import data_trimmer
from data_post_processing import data_post_processing
from argparse import ArgumentParser





def main(args):
    
    
    DATA_DIR,INPUT_DIR,OUTPUT_DIR,INTERMEDIATE_DIR = get_folders(args.machine)
    
    
    filename_sel  = join(INTERMEDIATE_DIR, "lookup_selected.xlsx")
    filename_look = join(INTERMEDIATE_DIR, "lookup.xlsx")
    
    
    
    # create lookup table
    if args.makelookup:
        generate_lookup(filename_look)
        print("The new lookup has been generated!")
        sys.exit(0)
        
        
    #load processes
    _, processes = get_processes(INPUT_DIR,filename_sel)
    
    # read Y (IST) file
    df_ist = pd.read_csv(join(INTERMEDIATE_DIR,"y_ist.csv"), sep=",")
    
    if args.devrun:
        test_id = df_ist["id"].unique()[:100]
        df_ist = df_ist.set_index("id").loc[test_id].reset_index()
    
    
    if args.runprocess:
        # get the booking file
        df_book = get_booking(INPUT_DIR)
        
        # get process sequence
        df_pc, df_book_mis,_ = sequence_builder(df_query=df_book.copy(), df_keys=df_ist.copy(), keys_branches=["SapNummer","Version","WA","id"],
                            processes=processes, saving_path=INTERMEDIATE_DIR, filename_sel=filename_sel, save_file=True)
        
        # check dimensions and trim
        df_ist_tr = data_trimmer(df_x=df_pc.copy(), df_y=df_ist.copy(), df_miss=df_book_mis.copy(), 
                                save_path=INTERMEDIATE_DIR, save_file=True)
        
    else:
        df_pc = pd.read_csv(join(INTERMEDIATE_DIR,"x_prochain.csv"), sep=",")
        df_book_mis = pd.read_csv(join(INTERMEDIATE_DIR, "booking_missing.csv"), sep=",")
        df_pro_mis = pd.read_csv(join(INTERMEDIATE_DIR, "process_missing.csv"), sep=",")
        
    
    # check that ids are aligned
    df_pc = df_pc.sort_values("id")
    df_ist_tr = df_ist_tr.sort_values("id")
    
    if all(df_pc["id"].drop_duplicates().to_numpy() == df_ist_tr["id"].drop_duplicates().to_numpy()):
        print("All IDs are aligned! Proceed conversion to numpy arrays")
    
    
        # post processing and conversion to numpy
        X_np = data_post_processing(df=df_pc, time_label="Time",id_label="id",sort_label="PaPos",features=["Value","Pos"],max_seq_len=1515)
        Y_np = data_post_processing(df=df_ist_tr, time_label="CreateDate",id_label="id",sort_label="Zyklus",features=["Value","Zyklus"],max_seq_len=250)
    
        # save numpy arrays
        with open(join(OUTPUT_DIR, "X_np.npy"), 'wb') as f:
            np.save(f, X_np)
    
        with open(join(OUTPUT_DIR, "Y_np.npy"), 'wb') as f:
            np.save(f, Y_np)
            
        print("Dataset files successfully generated.")
        print(f"X_np shape: {X_np.shape}, Y_np shape: {Y_np.shape}")
        
        
if __name__ == '__main__':
    parser = ArgumentParser(
        prog='Process Pipeline',
        description='The program builds sequential datasets from processes and booking table',
        epilog='Text at the bottom of help'
    )
    # parser.add_argument("--debug", default=False)
    parser.add_argument("-dr","--devrun",
                        type=bool,
                        choices=[True,False],
                        default=False,
                        help='Run a quick test for debugging purpose')
    
    parser.add_argument("-m","--machine", 
                        type=str,
                        choices=["local","cluster"],
                        default="local",
                        help="select local or cluster")
    
    parser.add_argument("-mkl","--makelookup", 
                        type=bool,
                        choices=[True,False],
                        default=False,
                        help="generate new lookup table and exit")
    
    parser.add_argument("-rp","--runprocess", 
                        type=bool,
                        choices=[True,False],
                        default=True,
                        help="build the process chains")
    
    args = parser.parse_args()
    main(args)
        