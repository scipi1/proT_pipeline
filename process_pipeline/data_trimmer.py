import numpy as np

def data_trimmer(df_x, df_y, df_miss, save_path:str ,save_file:bool=False):
    
    len_Y = len(df_y["id"].unique()) 
    len_X = len(df_x["id"].unique())
    
    
    # seen missing id during the assembly of the dataset
    seen_missing_id = []
    
    for i in df_miss.index:
        
        sap = df_miss.at[i,"Design"]
        ver = df_miss.at[i,"Version"]
        wa = df_miss.at[i,"Batch"]
    
        if ver == "ALL":
            mis = df_y.set_index(["SapNummer"]).loc[sap]["id"].unique().tolist()
                
        elif wa == "ALL":
            mis = df_y.set_index(["SapNummer","Version"]).loc[sap].loc[ver]["id"].unique().tolist()
            
        elif ver != "ALL" and wa != "ALL":
            mis = df_y.set_index(["SapNummer","Version","WA"]).loc[sap].loc[ver].loc[wa]["id"].unique().tolist()
            
        seen_missing_id.extend(mis)
    
    
    # actual missing id
    actual_missing_id = []
    for i in df_y["id"].unique():
        if i not in df_x["id"].tolist():
            actual_missing_id.append(i)
            
            
    # ID not seen from the method
    unseen_id = []
    for i in actual_missing_id:
        if i not in seen_missing_id:
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
        df_y_tr = df_y[np.logical_not([df_y["id"].iloc[i] in seen_missing_id for i in df_y.index])]
        df_y_tr = df_y_tr.sort_values("id")
        
        if df_y_tr["id"].all() == df_x["id"].all():
            new_Y_length = len(df_y_tr["id"].unique())
            print(f"The sizes match! Y size {new_Y_length} = {len_X} X size. The new trimmed Y file has been saved!")
            
            if save_file:
                df_y_tr.to_csv(save_path + "y_trimmed.csv", sep=",")
            
        else:
            raise ValueError("The X and Y size still don't match!")
        
    else:
        raise ValueError("The result of the previous cell was negative, fix it before proceeding!")
    
    return df_y_tr