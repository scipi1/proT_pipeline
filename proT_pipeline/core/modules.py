import numpy as np
import pandas as pd
import logging
from proT_pipeline.labels import *
from proT_pipeline.utils import safe_read_csv
from tqdm import tqdm
import os
from os.path import join
from typing import Tuple, List
import re


# used in assemble_raw.py
def split_queries_by_keys(query_groups:list, keys_groups:list)-> Tuple[List[str]]:
    """
    Finds keys which can ba queried and ones that are missing
    Args:
        query_groups (list): list of groups to be queried
        keys_groups (list): list of keys to query from

    Returns:
        Tuple[List[str]]: list of missing keys, list of available keys
    """

    exact_keys = set()
    wildcard_ids = set()

    for key in keys_groups:
        if re.fullmatch(r'[a-zA-Z0-9]+_\*', str(key)):      # if keys is of the format id_* (like multibond and microetch)
            wildcard_ids.add(key[:-2])                      # extract ID part from from id_* and add it to wildcard_ids set
        else:   
            exact_keys.add(key)                             

    missing_groups = []
    available_groups = []

    for query in query_groups:
        if query in exact_keys:
            available_groups.append(query)
        else:
            id_part = query.split('_')[0]
            if id_part in wildcard_ids:
                available_groups.append(query)
            else:
                missing_groups.append(query)

    return missing_groups, available_groups



def get_df_lookup(obj):
    """Generates a pandas DataFrame with the columns information of the process in the
    input dictionary, which can be used as lookup table

    Args:
        obj (object): Process class object 

    Returns:
        pandas DataFrame: contains information about the process parameters
    """
    
    df = obj.df
    machine_label = obj.machine_label
    prefix = obj.prefix
    
    df_pro = pd.DataFrame(df.dtypes,columns=["dtype"])
    df_pro["variable"] = [prefix +"_"+ str(i) for i in range(len(df_pro))]
    
    if machine_label is not False:
        machines = df[machine_label].unique()
        machines = [x for x in machines if x is not np.nan]
        
        for machine in machines:
            df_pro[machine] = np.logical_not(np.array(df[df[machine_label]==machine].isna().mode()).flatten())
    
    df_pro ["Select"] = False
    df_pro = df_pro.reset_index()
    
    for i,par in enumerate(df_pro["index"]):
        if df[par].dtype.kind in 'biufc':
            
            df_pro.at[i,"min"] = df[par].min()
            df_pro.at[i,"max"] = df[par].max()
            df_pro.at[i,"mean"] = df[par].mean()
            df_pro.at[i,"std"] = df[par].std()
    return df_pro


def get_data_step(
    wa:str,
    step:int,
    processes:list,
    filepath_sel:str
    ):
    """Looks for the current step within the processes and assembles a 

    Args:
        WA (str): batch number
        step (_type_): _description_
        processes (_type_): _description_
        filename_sel (str,path): location of the selective lookup table 

    Raises:
        ValueError: the lookup table doesn't contain any selected parameter

    Returns:
        _type_: _description_
    """
    
    
    for pro in processes:
        
        df_temp = pd.DataFrame()
        missing = None
        
        process_name = pro.process_label
        
        if wa in pro.df[pro.WA_label].unique().tolist():
        
            df_cp = pro.df.set_index(pro.WA_label).loc[wa]
            step_list = np.array(df_cp[pro.PaPos_label])

            if step in step_list:

                # open the lookup table
                df_lookup = pd.read_excel(filepath_sel,sheet_name=pro.process_label)

                # handle multiple machines on the same dataframe
                if pro.machine_label is not None:
                    machine = df_cp.iloc[0][pro.machine_label]                     # get the right machine from the process df
                    df_lookup = df_lookup[df_lookup[machine]]

                date_label = [i for i in df_lookup["index"] if i in pro.date_label]     # get the data_label from the lookup df
                parameters = df_lookup[df_lookup["Select"]]["index"].tolist()              # import only selected parameters ...
                variables = df_lookup[df_lookup["Select"]]["variable"].tolist()

                # check if the parameters have been selected
                if len(parameters) == 0:
                    raise ValueError(f"No selected parameters in the {process_name} lookup table")

                values = df_cp[parameters].apply(np.mean).tolist()
                time = df_cp[date_label]

                
                
                if len(time) == 1:
                    time = list(np.array(time))*len(values)
                else:
                    time = list(np.array(time.iloc[0]))*len(values)
                
                df_temp["Value"] = values
                df_temp["Time"] = time
                df_temp["Variable"] = variables
                df_temp["Process"] = process_name
                
                break

                
        else:
            missing = (process_name, wa, step)
    
    return df_temp, missing



def explode_time_components(df,time_label):
    
    time_components_labels = ["year","month","day","hour","minute"]
    df[time_label] = pd.to_datetime(df[time_label], errors="coerce")
    date_col = df[time_label].dt
    
    # Extract components in a single pass
    df[time_components_labels[0]] = date_col.year
    df[time_components_labels[1]] = date_col.month
    df[time_components_labels[2]] = date_col.day
    df[time_components_labels[3]] = date_col.hour
    df[time_components_labels[4]] = date_col.minute
    
    return df, time_components_labels



def pandas_to_numpy_ds(id_samples,df,features,id_labels,initial_max_seq_len,dtype_arr=float):
    """
    Generate a numpy dataset from a pandas dataframe 
    of the following shape: (#samples, sequence length, #features)

    Args:
        id_samples (list/array): id of samples to loop over, ideally same for input and target
        df (pd.Dataframe): dataframe to flatten into array
        features (list): columns of the df to select as features
        id_labels (list): label used for the id column
        initial_max_seq_len (_type_): max sequence length to start with, sequence will be pruned if too big

    Raises:
        ValueError: initial_max_seq_len is too big

    Returns:
        array/list: either the stacked final dataset array or the list of sample arrays if stacking unsuccessful 
    """

    
    logger = logging.getLogger(__name__)
    
    
    sample_list, seq_len_list = [], []
    id_list = []
    n_features = len(features)+1
    
    for id_ in tqdm(id_samples):
        
        
        # Select the rows for this ID, and convert to numpy
        sample_df = df.set_index(id_labels).loc[id_][features]        

        # convert to numpy
        try:
            sample_array = sample_df.reset_index().to_numpy(dtype=dtype_arr)
        except:
            sample_array = sample_df.reset_index()

        # save sequence length
        seq_len = len(sample_array)
        seq_len_list.append((seq_len,id_))
        
        # check if initial_max_seq_len is too small
        if seq_len > initial_max_seq_len:
            raise ValueError(f"Choose larger initial_max_seq_len, got {seq_len}")

        # pad with NaN if shorter
        if seq_len < initial_max_seq_len:
            padded_array = np.full((initial_max_seq_len, n_features), np.nan, dtype=dtype_arr) # create a template nan array
            padded_array[:seq_len] = sample_array  # overwrite beginning with actual data
            sample_array = padded_array

        sample_list.append(sample_array)
        id_list.append(id_)
        
    try:
        result_array = np.stack(sample_list, axis=0)
        max_len = max([tup[0] for tup in seq_len_list])
        result_array = result_array[:,:max_len]
        length_counts = pd.Series(seq_len_list).value_counts().sort_index()
        print("Flattening successful, dataset correctly generated!")
        print("Found the following sequence lengths")
        
        df = pd.DataFrame(seq_len_list, columns=["length", "id"])

        summary = (
            df.groupby("length").agg(
                length_count=("id", "size"),                 # how many ids
                ids=("id", lambda s: ", ".join(map(str, s))) # which ids
            ).sort_index()
        )
        
        print(summary)
        logger.info(f"Flattening successful: found the following sequence lengths")
        # logging.info(summary)
        
        for length, row in summary.iterrows():
            logger.info(
                "length_%d count=%d ids=%s",
                length,
                row.length_count,
                row.ids,                # already a comma-separated str
                )
        return result_array
        
    except:
        print("stacking didn't work")
        length_counts = pd.Series(seq_len_list).value_counts().sort_index()
        print("Found the following sequence lengths")
        print(length_counts)
        return sample_list



def filter_vars_max_missing(df: pd.DataFrame, threshold: float)->pd.DataFrame:
    """
    Filters process dataframe variables that contains a percentage of missing
    values > threshold

    Args:
        df (pd.DataFrame): non-filtered dataframe
        threshold (float): threshold

    Returns:
        pd.DataFrame: filtered dataframe
    """
    
    # calculate total percentage of missing values before filtering
    missing_percent_before = df[trans_value_label].isna().mean()*100

    # calculate % missing for each variable 
    missing_df = df.groupby(trans_variable_label).agg(
        {
            trans_value_label : lambda x: x.isna().mean() * 100,
        }).reset_index()
    
    # get variables where % missing < threshold
    filter_vars = missing_df[missing_df[trans_value_label] <= threshold][trans_variable_label]
    
    # filter variables
    df_filtered = df[df[trans_variable_label].isin(filter_vars)]
    
    # calculate total percentage of missing values after filtering
    missing_percent_after = df_filtered[trans_value_label].isna().mean()*100
    
    logging.info(f"Missing data filtered to a max threshold of {threshold} per variable")
    logging.info(f"Before {missing_percent_before} % missing, after {missing_percent_after} missing")

    return df_filtered



class Process():
    """
    Class to define a Process object, useful to store information of a given process and
    assemble the process chain.
    """
    def __init__(
        self,
        process_label: str,
        hidden_label : str,
        machine_label: str,
        WA_label: str,
        panel_label:str,
        PaPos_label:str,
        date_label:list,
        date_format : str,
        prefix : str,
        filename:str,
        sep: str,
        header: int
        ):
        self.process_label = process_label
        self.hidden_label = hidden_label
        self.machine_label = machine_label
        self.WA_label = WA_label
        self.panel_label = panel_label
        self.PaPos_label = PaPos_label
        self.date_label = date_label
        self.date_format = date_format
        self.prefix = prefix
        self.filename = filename
        self.sep = sep
        self.header = header
        self.flag = 0
        self.missing_columns = []
        
        
    def get_df(self,input_data_path):
        
        _, ext = os.path.splitext(self.filename)
        ext = ext.lower()
        
        if ext == '.csv':
            self.df = safe_read_csv(join(input_data_path,self.filename),sep=self.sep, header=self.header)
            self.flag = 1
        elif ext in ['.xls', '.xlsx', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            self.df = pd.read_excel(join(input_data_path,self.filename),header=self.header)
            self.flag = 1
        else:
            raise ValueError(f"Unsupported file extension: {ext}")        
        
    def normalize_df(self,filename_sel):
        if self.flag == 0:
            raise ValueError("First call get_df to initialize the dataframe!")
        
        self.df_lookup = pd.read_excel(filename_sel,sheet_name=self.process_label)
        self.parameters = self.df_lookup[self.df_lookup["Select"]]["index"]

        for p in self.parameters:
            if p not in self.df.columns:
                warning_str = f"{self.process_label} WARNING: {p} not in the columns"
                print(warning_str)
                logging.info(warning_str)
                self.missing_columns.append(p)
            try:
                self.df[p] = (self.df[p]-self.df[p].min())/(self.df[p].max()-self.df[p].min()+1E-6)
            except Exception as e:
                print(f"Error occurred {e}")
            
    def convert_timestamp(self):
        if self.flag == 0:
            raise ValueError("First call get_df to initialize the daraframe!")
        
        for d in self.date_label:
            self.df[d] = pd.to_datetime(self.df[d],format=self.date_format)
            
    def get_variables_list(self, filename_sel)->None:
        self.df_lookup = pd.read_excel(filename_sel,sheet_name=self.process_label)
        self.variables_list = self.df_lookup[self.df_lookup["Select"]]["variable"].tolist()
