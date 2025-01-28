import numpy as np
import pandas as pd
import os


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
    # df = df.copy()
    time_components_labels = ["Year","Month","Day","Hour","Minute"]
    
    # df[time_components_labels[0]] = pd.DatetimeIndex(df[time_label]).year
    # df[time_components_labels[1]] = pd.DatetimeIndex(df[time_label]).month
    # df[time_components_labels[2]] = pd.DatetimeIndex(df[time_label]).day
    # df[time_components_labels[3]] = pd.DatetimeIndex(df[time_label]).hour
    # df[time_components_labels[4]] = pd.DatetimeIndex(df[time_label]).minute
    df[time_label] = pd.to_datetime(df[time_label], errors="coerce")
    date_col = df[time_label].dt
    
    # Extract components in a single pass
    df[time_components_labels[0]] = date_col.year
    df[time_components_labels[1]] = date_col.month
    df[time_components_labels[2]] = date_col.day
    df[time_components_labels[3]] = date_col.hour
    df[time_components_labels[4]] = date_col.minute
    
    return df, time_components_labels







class Process():
    def __init__(
        self,
        process_label: str,
        hidden_label : str,
        machine_label: str,
        WA_label: str,
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
        self.PaPos_label = PaPos_label
        self.date_label = date_label
        self.date_format = date_format
        self.prefix = prefix
        self.filename = filename
        self.sep = sep
        self.header = header
        self.flag = 0
        
    def get_df(self,input_data_path):
        self.df = pd.read_csv(os.path.join(input_data_path,self.filename), sep=self.sep,header=self.header,low_memory=False)
        self.flag = 1
        
    
        
    def normalize_df(self,filename_sel):
        if self.flag == 0:
            raise ValueError("First call get_df to initialize the daraframe!")
        
        self.df_lookup = pd.read_excel(filename_sel,sheet_name=self.process_label)
        self.parameters = self.df_lookup[self.df_lookup["Select"]]["index"]

        for p in self.parameters:
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