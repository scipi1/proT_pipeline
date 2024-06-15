# RUNTIME c.a. 225min (without verbose)
import pandas as pd
from os.path import join
from tqdm import tqdm
from core.modules import get_data_step
from utils import nested_dict_from_pandas


def sequence_builder(df_query, df_keys, keys_branches:list, processes:list, saving_path:str, filename_sel:str
                    ,save_file:bool=False, verbose:bool=False):
    
    """Build a sequential dataset from a process chain
    
    Arguments:
        df_query (pandas DataFrame): DataFrame to query the tree leaves from
        df_keys (pandas DataFrame) : DataFrame containing the keys to be queried
        keys_branches (list): List of labels to assemble the keys tree
    
    Returns:
        _type_: _description_
    """
    # create a nested dictionary from Y (IST) queries
    d = nested_dict_from_pandas(df_keys.set_index(keys_branches)) #["SapNummer","Version","WA","id"]
    
    verboseprint = print if verbose else lambda *a, **k: None

    #tot_WA = len(df_keys["WA"].unique())
    #prog_bar = tqdm(total=tot_WA)

    booking_mis = [] # these samples are present in the output but are missing in the booking file
    process_mis = [] # these samples are present in the output but are missing in the process(es) file(s)
    df_pc = pd.DataFrame()     # initiate the output process chain DataFrame
    
    for s in  tqdm(d.keys()):
        df_sel = df_query.set_index(["SAP"])
        
        if s in df_sel.index:
        
            for v in d[s].keys():
                df_sel = df_query.set_index(["SAP","SAP_Version"]).loc[int(s)]
                
                if v in df_sel.index:
                
                    for wa in d[s][v].keys():
                        #prog_bar.update()
                        
                        df_sel = df_query.set_index(["SAP","SAP_Version","WA"]).loc[int(s)].loc[v]
                        
                        if wa in df_sel.index:
                            
                            df_sel = df_query.set_index(["SAP","SAP_Version","WA"]).loc[int(s)].loc[v].loc[[wa]]
                            
                            verboseprint(f"Current batch: {wa}")
                            df_wa = pd.DataFrame()
                            
                            # STEP 1 COMPUTE PROCESS CHAIN OF THE GIVEN BATCH
                            step_counter = 0
                            for step in df_sel["PaPosNumber"]:
                                
                                df_temp, mis = get_data_step(wa,step,processes,filename_sel)       # get data for the current WA, Version and PaPosNumber
                                
                                if mis is not None:
                                    process_mis.append(mis)
                                
                                if df_temp is not None:
                                    df_temp["WA"] = wa                                             # append current design
                                    df_temp["PaPos"] = step                                        # append current step
                                    df_temp["Version"] = v                                         # append current version
                                    df_temp["SAP"] = s                                             # append current design
                                    df_temp["Pos"] = step_counter                                  # append current step counter
                                    
                                    step_counter += 1                                              # increment for next step
    
                                    if len(df_wa) == 0:                                            # first loop --> initiate
                                        df_wa = df_temp.copy()
                                        verboseprint("Process Dataframe initialized!")
                                        
                                    else:
                                        df_wa = pd.concat([df_wa,df_temp])                        # from second loop on...append
                                        verboseprint(f"Process Dataframe updated...{wa},{step}")
                                    
                            # STEP 2 DUPLICATE AND APPEND FOR ALL COUPONS IN THE SET
                            if len(df_wa)> 0:
                                id_list = d[s][v][wa]                                              # ID coupons belonging to the same batch/version/design
                                
                                for id in id_list:
                                    df_wa["id"] = id                                               # add ID column with current coupon ID
                                    df_wa["Position"] = [i for i in range(len(df_wa))]
                                    
                                    if len(df_pc) == 0:                                            # first loop --> initiate
                                        df_pc = df_wa.copy()
                                        
                                    else:                                                          # from second loop on...append
                                        df_pc = pd.concat([df_pc,df_wa])
                            
                            else:
                                booking_mis.append((s,v,wa))
                        else:
                            booking_mis.append((s,v,wa))
                else:
                    booking_mis.append((s,v,"ALL"))
        else:
            booking_mis.append((s,"ALL","ALL"))
            
    if df_pc is None:
        raise ValueError("No process has been found for the selected output!")            
    
    # assemble DataFrame of missing data
    df_book_mis = pd.DataFrame(booking_mis, columns=["Design","Version","Batch"])
    df_pro_mis = pd.DataFrame(process_mis, columns=["Process", "Batch", "PaPos"])
    
    
    if save_file:
        # save report of missing data
        df_book_mis.to_csv(join(saving_path, "booking_missing.csv"), sep=",")
        df_pro_mis.to_csv(join(saving_path,"process_missing.csv"), sep=",")
        df_pc.to_csv(saving_path + "x_prochain.csv", sep=",")
        
    return df_pc, df_book_mis,df_pro_mis