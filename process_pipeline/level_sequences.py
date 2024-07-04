import pandas as pd
import numpy as np
from os.path import join
from utils import nested_dict_from_pandas


def level_sequences(
    df: pd.DataFrame,
    design_label: str = "SAP",
    version_label: str = "Version",
    variable_label: str = "Variable",
    process_label: str = "Process",
    batch_label: str = "WA",
    step_label: str = "PaPos",
    id_label: str = "id",
    abs_pos_label: str = "AbsPos",
    given_label: str = "Given",
    value_label: str = "Value",
    ):
    
    # create a nested dictionary from Y (IST) queries
    multi_idx = [design_label,version_label,batch_label,id_label]
    d = nested_dict_from_pandas(df.set_index(multi_idx))

    # get the reference batches
    ref_batches = []
    len_list = []

    for design in d.keys():

        for version in d[design].keys():
            max_len = 0

            for batch in d[design][version].keys():

                for id in d[design][version][batch]:
                    l = len(df.set_index(multi_idx).loc[design,version,batch][step_label].unique())
                    len_list.append(l)
                    if l >= max_len:
                        max_len = l
                        ref_batch = batch
                        ref_id = id

            ref_batches.append((design,version,ref_batch,ref_id))

    # assemble the hash DataFrame
    df_hash = None
    df_template = None
    template_cols = [design_label,version_label,step_label,abs_pos_label,variable_label,process_label]
    h_multi_idx = [design_label,version_label,step_label]

    for ref_batch in ref_batches:
        df_hash_temp = df.set_index(multi_idx).loc[ref_batch][step_label].sort_values()
        df_hash_temp = df_hash_temp.drop_duplicates().reset_index()
        df_hash_temp["AbsPos"] = np.arange(len(df_hash_temp))
        df_template_temp = df.set_index(multi_idx).loc[ref_batch].sort_values(by=[step_label,variable_label])
        df_template_temp = df_template_temp.reset_index()

        if df_hash is not None:
            df_hash = pd.concat([df_hash,df_hash_temp],ignore_index=True)

        if df_template is not None:
            df_template = pd.concat([df_template,df_template_temp],ignore_index=True)

        if df_hash is None:
            df_hash = df_hash_temp

        if df_template is None:
            df_template = df_template_temp


    # get absolute position in the templates
    abs_pos_list = []

    for ix in df_template.index:
            coordinate = tuple(df_template.iloc[ix][h_multi_idx])
            abs_pos = df_hash.set_index(h_multi_idx).loc[coordinate][abs_pos_label]
            abs_pos_list.append(abs_pos)

    df_template[abs_pos_label] = abs_pos_list
    df_template = df_template[template_cols]


    # level all sequences
    df_lev = None

    for design in d.keys():

        for version in d[design].keys():
            template = df_template.set_index([design_label,version_label,step_label,variable_label]).loc[design,version]


            for batch in d[design][version].keys():

                for id in d[design][version][batch]:
                    _df = df.set_index([design_label,version_label,batch_label,id_label,step_label,variable_label]).sort_index().loc[design,version,batch,id]
                    _df = _df.drop([process_label], axis=1)

                    df_lev_temp = pd.concat([template,_df],axis=1)

                    # place back information
                    df_lev_temp = df_lev_temp.reset_index()
                    df_lev_temp[design_label] = design
                    df_lev_temp[version_label] = version
                    df_lev_temp[batch_label] = batch
                    df_lev_temp[id_label] = id

                    if df_lev is None:
                        df_lev = df_lev_temp
                    else:
                        df_lev = pd.concat([df_lev,df_lev_temp],ignore_index=True)

    df_lev[given_label] = df_lev[value_label].notna().astype(int)
    
    return df_lev