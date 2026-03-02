import pandas as pd
import numpy as np
import logging
from proT_pipeline.input_processing.data_loader import get_processes
from proT_pipeline.labels import *
from proT_pipeline.core.modules import split_queries_by_keys
from proT_pipeline.utils import safe_read_csv
import json
import re
from os.path import join, exists
from os import makedirs
from typing import Tuple, List, Literal, Dict



def validate_class_columns(df_lookup: pd.DataFrame, process_label: str) -> Dict[str, int]:
    """
    Validate that each selected parameter has exactly one class (Read XOR Set).
    
    Args:
        df_lookup (pd.DataFrame): Lookup table with Select, Read, and Set columns
        process_label (str): Process name for error messages
        
    Returns:
        dict: Mapping from parameter index to class index (1=Read, 2=Set)
        
    Raises:
        ValueError: If any parameter is in both categories or neither category
    """
    # Normalize column names: lowercase and strip whitespace (handles "Read ", "READ", etc.)
    df_lookup = df_lookup.rename(columns=lambda c: c.lower().strip())
    
    selected = df_lookup[df_lookup["select"]]
    
    # Handle potential NaN values - treat as not having an 'x'
    read_marked = selected["read"].fillna("").str.lower().str.strip() == "x"
    set_marked = selected["set"].fillna("").str.lower().str.strip() == "x"
    
    # Check for parameters in both categories
    both_mask = read_marked & set_marked
    if both_mask.any():
        raise ValueError(
            f"Process '{process_label}': Parameters marked in BOTH Read and Set categories: "
            f"{selected.loc[both_mask, 'index'].tolist()}"
        )
    
    # Check for parameters in neither category  
    neither_mask = ~read_marked & ~set_marked
    if neither_mask.any():
        raise ValueError(
            f"Process '{process_label}': Parameters not marked in either Read or Set category: "
            f"{selected.loc[neither_mask, 'index'].tolist()}"
        )
    
    # Create mapping: parameter -> class_index (1=Read, 2=Set)
    param_to_class = {}
    for idx, row in selected.iterrows():
        param_name = row["index"]
        if read_marked.loc[idx]:
            param_to_class[param_name] = 1  # Read
        else:
            param_to_class[param_name] = 2  # Set
    
    return param_to_class


def assemble_raw(
    dataset_id: str, 
    grouping_method:Literal["panel", "column"]="panel", 
    grouping_column: str=None, 
    selected_processes: List[str]=None,
    debug: bool=False
    )->None:
    
    """
    Assembles a raw dataframe containing process data from the single
    process files, according to control files which select
    - which variables for each process to add
    - which step (PaPos) to include
    
    The raw dataframe is finally saved.
    
    Args:
        dataset_id (str): working dataset folder
        grouping_method (Literal["panel", "column"]): method for grouping samples
        grouping_column (str, optional): column name if using column grouping
        selected_processes (List[str], optional): list of process labels to load 
            (e.g., ["Laser", "Plasma"]). If None, all processes are loaded.
        debug (bool): if True, process only first 100 samples
    """
    
    # ============================================================================
    # SETUP
    # ============================================================================
    ROOT_DIR = get_root_dir()
    INPUT_DIR, OUTPUT_DIR, CONTROL_DIR = get_dirs(ROOT_DIR, dataset_id)
    filepath_selected = join(CONTROL_DIR, selected_filename)
    filepath_target = join(CONTROL_DIR, target_filename)
    
    # Load processes with specified grouping method
    try:
        _, processes = get_processes(
            INPUT_DIR, 
            filepath_selected, 
            grouping_method, 
            grouping_column,
            selected_process_labels=selected_processes
        )
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Could not load process files. Check that input directory exists: {INPUT_DIR}\n"
            f"And that selected file exists: {filepath_selected}"
        ) from e

    # Read target (IST) file
    try:
        df_trg = safe_read_csv(filepath_target, sep=target_sep)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Target file not found: {filepath_target}\n"
            f"Make sure df_trg.csv exists in the control directory."
        )
    except pd.errors.EmptyDataError:
        raise ValueError(f"Target file is empty: {filepath_target}")
    query_groups = df_trg[trans_group_id].unique().tolist()
    
    if debug:
        query_groups = df_trg[trans_group_id].unique()[:100].tolist()
    
    # Create group_id to design_version mapping (if columns exist)
    # In prediction mode with placeholder targets, these columns may not exist
    if trans_design_label in df_trg.columns and trans_version_label in df_trg.columns:
        df_trg[trans_design_version_label] = df_trg[trans_design_label].astype(str) + "_" + df_trg[trans_version_label].astype(str)
        design_version_map = df_trg[[trans_group_id, trans_design_version_label]].set_index(trans_group_id).to_dict()
        use_design_version_mapping = True
    else:
        logging.warning(
            "Design/version columns not found in df_trg. "
            "Using placeholder value for design_version (prediction mode)."
        )
        design_version_map = None
        use_design_version_mapping = False
    
    # Load process step selection control file
    df_steps_sel = pd.read_excel(join(CONTROL_DIR, selected_process_filename))
    steps_sel = np.array(df_steps_sel[df_steps_sel['Select']]["Step"])

    # ============================================================================
    # PROCESS EACH PROCESS FILE
    # ============================================================================
    missing_groups_dic = {}
    df_list = []
    
    for pro in processes:
        # Load lookup table for current process
        df_lookup = pd.read_excel(filepath_selected, sheet_name=pro.process_label)
        date_labels = [i for i in df_lookup["index"] if i in pro.date_label]
        parameters = df_lookup[df_lookup["Select"]]["index"].tolist()
        variables = df_lookup[df_lookup["Select"]][trans_variable_label].tolist()

        assert len(parameters) == len(variables)
        params_vars = {parameters[i]: variables[i] for i in range(len(parameters))}
        
        # Validate and extract class information (Read/Set)
        params_class = validate_class_columns(df_lookup, pro.process_label)

        df_cp = pro.df
        
        # Track missing and available groups for this process
        keys_groups = df_cp[trans_group_id].unique().tolist()
        missing_groups, available_groups = split_queries_by_keys(query_groups, keys_groups)
        missing_groups_dic[pro.process_label] = missing_groups
        
        # Expand dataframe for processes without panel-level data
        if pro.process_label in ["Multibond", "Microetch"]:
            df_cp = group_expand_dataframe(df_cp, available_groups, keys_groups)
            
        logging.info(f"Processing {pro.process_label}")
        
        # Select only available batches
        df_cp = df_cp.set_index(trans_group_id).loc[available_groups].reset_index()
        
        # Parse datetime columns
        datetime_list = []
        for date_label in date_labels:
            try:
                date_time_col = pd.to_datetime(df_cp[date_label], format=pro.date_format)
            except:
                date_time_col = pd.to_datetime(df_cp[date_label], format="mixed")
            datetime_list.append(date_time_col)
        
        if len(datetime_list) > 1:
            logging.warning(f"Process {pro.process_label} has more than one date label, taking first one")
        
        df_cp[trans_date_label] = datetime_list[0]
        
        # Remove missing columns from parameters
        if len(pro.missing_columns) != 0:
            parameters = [p for p in parameters if p not in pro.missing_columns]
        
        # Reshape dataframe from wide to long format
        df_cp = df_cp.melt(
            id_vars=[trans_group_id, pro.PaPos_label, trans_date_label],
            value_vars=parameters,
            var_name=trans_parameter_label,
            value_name=trans_value_label)
        
        # Add variable, process, and class labels
        df_cp[trans_variable_label] = df_cp[trans_parameter_label].map(params_vars)
        df_cp[trans_process_label] = pro.process_label
        df_cp[trans_class_label] = df_cp[trans_parameter_label].map(params_class)
        
        # Standardize column names
        df_cp = df_cp.rename(columns={pro.PaPos_label: trans_position_label})
        
        if not df_cp.empty:
            df_list.append(df_cp)
    
    # ============================================================================
    # COMBINE AND FILTER
    # ============================================================================
    if len(df_list) == 0:
        raise ValueError("Zero process dataframe found, check your queries!")
    elif len(df_list) == 1:
        df_raw = df_list[0]
    else:
        df_raw = pd.concat(df_list, ignore_index=True)
    
    # Filter to selected steps from control file
    df_raw = df_raw[df_raw[trans_position_label].isin(steps_sel)]
    
    if df_raw.empty:
        raise ValueError("Selected steps produced empty dataframe. If debug mode, try increasing slice.")
    
    # Validate position-to-process uniqueness
    df_unique_pairs = df_raw[[trans_position_label, trans_process_label]].drop_duplicates().sort_values(by=trans_position_label)
    count_process_per_position = df_unique_pairs.groupby(trans_position_label)[trans_process_label].nunique()
    df_check = count_process_per_position[count_process_per_position > 1]
    
    if len(df_check) != 0: 
        logging.warning(f"Action needed! {len(df_check)} position IDs are used for multiple processes: {df_check.index.tolist()}")
    
    # Apply design_version mapping (or placeholder for prediction mode)
    if use_design_version_mapping:
        df_raw[trans_design_version_label] = df_raw[trans_group_id].map(design_version_map[trans_design_version_label])
    else:
        # In prediction mode without design/version info, use placeholder
        df_raw[trans_design_version_label] = "UNKNOWN_UNKNOWN"
    
    # ============================================================================
    # SAVE OUTPUTS
    # ============================================================================
    if not exists(OUTPUT_DIR):
        makedirs(OUTPUT_DIR)
    
    df_raw.to_csv(join(OUTPUT_DIR, trans_df_process_raw))
    with open(join(OUTPUT_DIR, trans_missing_batches), "w") as f:
        json.dump(missing_groups_dic, f, indent=4)
    
    # Save class vocabulary dictionary (1=Read, 2=Set)
    class_dict = {1: class_read_label, 2: class_set_label}
    with open(join(OUTPUT_DIR, class_dict_filename), "w") as f:
        json.dump(class_dict, f, indent=4)






def group_expand_dataframe(df_cp, available_groups, key_groups):
    """
    Expand dataframe for processes without panel information.
    
    Some processes (Multibond, Microetch) don't have panel-level data.
    This function replicates their batch-level measurements for all
    panels that should have been processed in that batch.
    
    Args:
        df_cp (pd.DataFrame): Process dataframe with wildcard groups (batch_*)
        available_groups (list): List of specific panel groups (e.g., batch_1, batch_2)
        key_groups (list): List of all group keys in the process data
    
    Returns:
        pd.DataFrame: Expanded dataframe with replicated data for each panel
    """
    df_ = df_cp.copy()
    df_list = []
    for av_group in available_groups:
        # Convert specific panel group to wildcard format
        av_group_unexpanded = av_group.split("_")[0] + "_*"
        df_temp = df_.set_index(trans_group_id).loc[[av_group_unexpanded]].reset_index()
        df_temp["new_group_temp"] = av_group
        df_list.append(df_temp)
    
    df_long = pd.concat(df_list)
    df_long.reset_index(inplace=True)
    df_long[trans_group_id] = df_long["new_group_temp"]
    
    return df_long




if __name__ == "__main__":
    assemble_raw(dataset_id = "dx_occurrence_test")
