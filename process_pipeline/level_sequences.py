import pandas as pd
import numpy as np
from os.path import join,dirname,abspath
from typing import List

import sys
ROOT_DIR = dirname(dirname(abspath(__file__)))
sys.path.append(ROOT_DIR)
from process_pipeline.utils import nested_dict_from_pandas
from process_pipeline.core.modules import Process
from process_pipeline.labels import *


def get_template(df: pd.DataFrame, processes = List[Process]):
    
    # create a nested dictionary from Y (IST) queries
    multi_idx = [input_design_label,input_version_label,input_batch_label,input_id_label,input_step_label]
    d = nested_dict_from_pandas(df.set_index(multi_idx))

    # get the reference batches
    outer_dict = {}

    for design in d.keys():
        middle_dict = {}

        for version in d[design].keys():
            inner_dict = {}

            # scan all possible batch/id
            #----------------------------------------------------------------------------------------------------------------------
            for batch in d[design][version].keys():

                for id in d[design][version][batch].keys():
                    # steps = df.set_index(multi_idx).loc[design,version,batch,id][step_label].unique()
                    
                    for step in d[design][version][batch][id]:
                        if step not in inner_dict.keys():
                            process = df.set_index(multi_idx).loc[design,version,batch,id,step][input_process_label].value_counts().index[0]
                            var_list = [pro.variables_list for pro in processes if pro.process_label == process][0]
                            inner_dict[step] = {input_process_label:process,input_variable_label:var_list,input_abs_pos_label:0}
            #----------------------------------------------------------------------------------------------------------------------
            
            for i, key in enumerate(sorted(inner_dict.keys())):
                var_dict = dict()
                for var in inner_dict[key][input_variable_label]:
                    var_dict[var] = {
                        input_abs_pos_label:i,
                        input_process_label:inner_dict[key][input_process_label]}
                inner_dict[key] = var_dict
                
            middle_dict[version] = inner_dict
        
        outer_dict[design] = middle_dict
    
    return outer_dict


def level_sequences(df: pd.DataFrame, processes: List[Process],save_dir: str):
    
    # create a nested dictionary from Y (IST) queries
    multi_idx = [input_design_label,input_version_label,input_batch_label,input_id_label]
    d = nested_dict_from_pandas(df.set_index(multi_idx))

    
    templates = get_template(df=df,processes=processes)
    df_templates = get_df_templates(templates_dict=templates)
    df_templates = df_templates.set_index([templates_design_label,templates_version_label])
    print("Template assembled and saved")
    
    # get absolute position in the templates
    df_lev = None
    max_seq_len = 0
    
    for design in d.keys():

        for version in d[design].keys():
            
            df_template = df_templates.loc[design].loc[version].reset_index()
            df_template = df_template.set_index([templates_step_label,input_process_label,templates_variable_label]).sort_index()
            
            len_template = len(df_template)
            if len_template>max_seq_len:
                max_seq_len = len(df_template)
                
            for batch in d[design][version].keys():

                for id in d[design][version][batch]:
                    _df = df.set_index([input_design_label,input_version_label,input_batch_label,input_id_label]).loc[design,version,batch,id].reset_index()
                    _df = _df.drop_duplicates(subset=[input_step_label,input_variable_label])
                    _df = _df.set_index([input_step_label,input_process_label,input_variable_label]).sort_index()
                    assert len(_df) <= len_template, "Error in the template generation, it's not the longest"
                    df_lev_temp = pd.concat([df_template,_df],axis=1)
                    assert len(df_lev_temp) <= len_template, f"Error in the concatenation for {design}{version}{batch}{id}"
                    df_lev_temp = df_lev_temp.reset_index().rename(columns={'level_0':input_step_label, "level_1":input_variable_label})
                    assert len(df_lev_temp) <= len_template, "Error in the reset index"
                    df_lev_temp[input_design_label] = design
                    df_lev_temp[input_version_label] = version
                    df_lev_temp[input_batch_label] = batch
                    df_lev_temp[input_id_label] = id
                    
                    if df_lev is None:
                        df_lev = df_lev_temp
                    else:
                        df_lev = pd.concat([df_lev,df_lev_temp],axis=0,ignore_index=True)
                
                        

    df_lev[input_given_label] = df_lev[input_value_label].notna().astype(int)
    
    return df_lev,max_seq_len,df_templates


def get_df_templates(templates_dict: dict):
    df_templates = None
    for design in templates_dict.keys():
        for version in templates_dict[design].keys():
            sel_template = templates_dict[design][version]

            df_temp = pd.DataFrame.from_dict(
                {
                    (i,j): sel_template[i][j] 
                    for i in sel_template.keys()
                    for j in sel_template[i].keys()
                    },
                orient="index")

            df_temp = df_temp.reset_index().rename(columns={'level_0': templates_step_label, 'level_1': templates_variable_label})
            df_temp[templates_design_label] = design
            df_temp[templates_version_label] = version

            if df_templates is None:
                df_templates = df_temp
            else:
                df_templates = pd.concat([df_templates,df_temp],axis=0)
    return df_templates