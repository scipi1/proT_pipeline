# RUNTIME c.a. 225min (without verbose)
import pandas as pd
from os.path import join
from tqdm import tqdm
from core.modules import get_data_step
from utils import nested_dict_from_pandas
from labels import *


def sequence_builder(
    df_key:pd.DataFrame, 
    df_query:pd.DataFrame, 
    query_branches:tuple, 
    processes:list,
    selected_steps:list,
    filepath_selected:str, 
    verbose:bool=False):
    
    """Build a sequential dataset from a process chain
    
    Arguments:
        df_query (pandas DataFrame): DataFrame to query the tree leaves from
        df_keys (pandas DataFrame) : DataFrame containing the keys to be queried
        keys_branches (list): List of labels to assemble the keys tree
    
    Returns:
        _type_: _description_
    """
    # create a nested dictionary from Y (IST) queries
    d = nested_dict_from_pandas(df_query.set_index([i for i in query_branches]))
    
    verboseprint = print if verbose else lambda *a, **k: None

    #tot_WA = len(df_keys["WA"].unique())
    #prog_bar = tqdm(total=tot_WA)

    booking_mis = [] # these samples are present in the output but are missing in the booking file
    process_mis = [] # these samples are present in the output but are missing in the process(es) file(s)
    df_prochain = pd.DataFrame()     # initiate the output process chain DataFrame
    
    for design in  tqdm(d.keys()):
        df_sel = df_key.set_index([booking_design_label])
        
        if design in df_sel.index:
        
            for version in d[design].keys():
                df_sel = df_key.set_index([booking_design_label,booking_version_label]).loc[int(design)]
                
                if version in df_sel.index:
                
                    for batch in d[design][version].keys():
                        
                        df_sel = df_key.set_index([booking_design_label,booking_version_label,booking_batch_label]).loc[int(design)].loc[version]
                        
                        if batch in df_sel.index:
                            
                            df_sel = df_key.set_index([booking_design_label,booking_version_label,booking_batch_label]).loc[int(design)].loc[version].loc[[batch]]
                            
                            verboseprint(f"Current batch: {batch}")
                            df_batch = pd.DataFrame()
                            
                            # STEP 1 COMPUTE PROCESS CHAIN OF THE GIVEN BATCH
                            for step in df_sel[booking_step_label]:
                                
                                if step in selected_steps:
                                
                                    df_temp, mis = get_data_step(batch,step,processes,filepath_selected)       # get data for the current WA, Version and PaPosNumber
                                    df_temp = df_temp.drop_duplicates(subset=input_variable_label)               # for each step, variables repeat once
                                    
                                    if mis is not None:
                                        process_mis.append(mis)
                                    
                                    if df_temp is not None:
                                        df_temp[input_batch_label] = batch                                             # append current design
                                        df_temp[input_step_label] = step                                        # append current step
                                        df_temp[input_version_label] = version                                         # append current version
                                        df_temp[input_design_label] = design                                             # append current design                                    
        
                                        if len(df_batch) == 0:                                            # first loop --> initiate
                                            df_batch = df_temp.copy()
                                            verboseprint("Process Dataframe initialized!")
                                            
                                        else:
                                            df_batch = pd.concat([df_batch,df_temp])                        # from second loop on...append
                                            verboseprint(f"Process Dataframe updated...{batch},{step}")
                                    
                            # STEP 2 DUPLICATE AND APPEND FOR ALL COUPONS IN THE SET
                            if len(df_batch)> 0:
                                id_list = d[design][version][batch]                                              # ID coupons belonging to the same batch/version/design
                                
                                for id in id_list:
                                    df_batch_id = df_batch.copy()
                                    df_batch_id[target_id_label] = id                                               # add ID column with current coupon ID
                                    
                                    if len(df_prochain) == 0:                                            # first loop --> initiate
                                        df_prochain = df_batch_id
                                        
                                    else:                                                          # from second loop on...append
                                        df_prochain = pd.concat([df_prochain,df_batch_id])
                            
                            else:
                                booking_mis.append((design,version,batch))
                        else:
                            booking_mis.append((design,version,batch))
                else:
                    booking_mis.append((design,version,"ALL"))
        else:
            booking_mis.append((design,"ALL","ALL"))
            
    if df_prochain is None:
        raise ValueError("No process has been found for the selected output!")            
    
    # assemble DataFrame of missing data
    df_book_mis = pd.DataFrame(booking_mis, columns=[input_design_label,input_version_label,input_batch_label]) #*!TD old["Design","Version","Batch"]
    df_pro_mis = pd.DataFrame(process_mis, columns=[input_process_label, input_batch_label, input_step_label]) #*! ["Process", "Batch", "PaPos"]
        
    return df_prochain, df_book_mis,df_pro_mis