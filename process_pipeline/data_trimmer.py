import numpy as np
from labels import *

def data_trimmer(df_x, df_y, df_miss, save_path:str ,save_file:bool=False):
    
    len_Y = len(df_y[target_id_label].unique()) 
    len_X = len(df_x[input_id_label].unique())
    
    # seen missing id during the assembly of the dataset
    seen_missing_id = []
    
    for i in df_miss.index:
        
        sap = df_miss.at[i,input_design_label]
        ver = df_miss.at[i,input_version_label]
        wa = df_miss.at[i,input_batch_label]
    
        if ver == "ALL":
            mis = df_y.set_index([target_design_label]).loc[sap][target_id_label].unique().tolist()
                
        elif wa == "ALL":
            mis = df_y.set_index([target_design_label,target_version_label]).loc[sap].loc[ver][target_id_label].unique().tolist()
            
        elif ver != "ALL" and wa != "ALL":
            mis = df_y.set_index([target_design_label,target_version_label,target_batch_label]).loc[sap].loc[ver].loc[wa][target_id_label].unique().tolist()
            
        seen_missing_id.extend(mis)
    
    
    # actual missing id
    actual_missing_id = []
    for i in df_y[target_id_label].unique():
        if i not in df_x[input_id_label].tolist():
            actual_missing_id.append(i)
            
            
    # ID not seen from the method
    unseen_id = []
    for i in actual_missing_id:
        if i not in seen_missing_id: #and i not in cut_id:
            unseen_id.append(i)
    
    if len(unseen_id) == 0:
        print("Successful: all missing data are intercepted! Proceeding trimming Y...")
        print(f"Elements in X {len_X} + {len(seen_missing_id)} missing = {len_X+len(seen_missing_id)} = {len_Y} elements in Y")
        pass_flag = True
        
    else:
        print("Problem: The method did not intercept all missing data...")
        print(f"The following {unseen_id} are missing")
        pass_flag = False
    
    # trim and save    
    if pass_flag:
        #df_y_tr = df_y[np.logical_not([df_y["id"].iloc[i] in seen_missing_id for i in df_y.index])]
        df_y_tr = df_y[[df_y[target_id_label].iloc[i] not in seen_missing_id for i in df_y.index]]
        df_y_tr = df_y_tr.sort_values(target_id_label)
        
        if df_y_tr[target_id_label].all() == df_x["id"].all():
            new_Y_length = len(df_y_tr[target_id_label].unique())
            print(f"The sizes match! Y size {new_Y_length} = {len_X} X size. The new trimmed Y file has been saved!")
            
            if save_file:
                df_y_tr.to_csv(save_path+target_trimmed_filename, sep=standard_sep)
            
        else:
            raise ValueError("The X and Y size still don't match!")
        
    else:
        raise ValueError("The result of the previous cell was negative, fix it before proceeding!")
    
    return df_y_tr