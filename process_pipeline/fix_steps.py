import pandas as pd
from os.path import dirname, abspath
from labels import *

ROOT_DIR = dirname(dirname(abspath(__file__)))

DATA_DIR = join(ROOT_DIR,"data")
INTERMEDIATE_DIR = join(DATA_DIR,"intermediate")

df_pc = pd.read_csv(join(INTERMEDIATE_DIR,input_raw_filename))

def reset_dict(monitor_list):
    monitor_dict = dict()
    for item in monitor_list:
        monitor_dict[item] = 0
    return monitor_dict

def update_dict(monitor_dict,df,item_label)->None:
    for _, row in df.iterrows():
        item = row[item_label]
        monitor_dict[item] += 1
        
def get_df_double(df,idnr_label,item_label):
    return df[[idnr_label,item_label]].sort_values(by=idnr_label).drop_duplicates(ignore_index=True)

def fix_steps(df):

    _df = get_df_double(df,input_step_label,input_process_label)

    value_counts = _df[input_step_label].value_counts()
    df_problem = _df[_df[input_step_label].isin(value_counts[value_counts > 1].index)]

    processes = _df[input_process_label].unique()
    steps = _df[input_step_label].unique()

    process_dict = reset_dict(processes)

    items_to_change = []
    while len(df_problem)>0:
        update_dict(process_dict,df_problem,input_process_label)
        worst_element = max(process_dict, key=process_dict.get)
        items_to_change.append(worst_element)
        steps_to_remove = df_problem[df_problem[input_process_label]==worst_element][input_step_label]
        df_problem = df_problem[~df_problem[input_step_label].isin(steps_to_remove)]

    mapping_dict = dict()


    for item_to_change in items_to_change:
        mapping_found = False
        const = 1
        if not mapping_found:
            new_steps = _df[_df[input_process_label]==item_to_change][input_step_label].apply(lambda x: x+const)
            if ~new_steps.isin(steps).all():
                mapping_dict[item_to_change] = const
                mapping_found = True
            else:
                const += 1

    # Function to add dictionary value to the input if the key exists
    def add_dict_value(x, key):
        return x + mapping_dict[key] if key in mapping_dict else x

    # Apply the function only to 'value_column', based on the 'key_column'
    df[input_step_label] = df.apply(lambda row: add_dict_value(row[input_step_label], row[input_process_label]), axis=1)
    
    return df
