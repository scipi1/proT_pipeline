import pandas as pd
import numpy as np
import logging
from os.path import join, exists
from os import mkdir
from proT_pipeline.core.modules import explode_time_components, pandas_to_numpy_ds
from proT_pipeline.utils import safe_read_csv
import json
from proT_pipeline.labels import *
from proT_pipeline.rarity_utils import *


def generate_dataset(dataset_id, split_input_by_class: bool = False):
    """
    Generate the final dataset from processed dataframes.
    
    This function performs several key operations:
    1. Loads processed input and target dataframes
    2. Creates vocabulary mappings (processes, variables, groups)
    3. Computes sample-level metrics for stratified splitting
    4. Defines feature schema for X and Y arrays
    5. Converts pandas DataFrames to numpy arrays
    6. Saves dataset as compressed npz file
    
    The dataset uses integer encoding for categorical features:
    - Group IDs: 0, 1, 2, ... (sample identifiers)
    - Processes: 1, 2, 3, ... (Laser, Plasma, etc.)
    - Variables: 1, 2, 3, ... (input and target parameters)
    
    Args:
        dataset_id (str): working dataset folder name
        split_input_by_class (bool): If True, split input data into S (Set params, class=2)
            and X (Read params, class=1), producing three tensors (S, X, Y) compatible
            with CausaliT format. Default False maintains backward compatibility with
            two tensors (X, Y).
    
    Raises:
        FileNotFoundError: If required input files are missing
    
    Outputs:
        - Vocabulary JSON files (groups, processes, variables, features)
        - Sample metrics parquet file
        - Compressed dataset npz file with X and Y arrays (or S, X, Y if split_input_by_class=True)
    """
    
    # Define directories
    ROOT_DIR = get_root_dir()
    _, OUTPUT_DIR, CONTROL_DIR = get_dirs(ROOT_DIR, dataset_id)
    filepath_target = join(CONTROL_DIR, target_filename)
    
    # Load input and target dataframes
    try:
        df_trg = safe_read_csv(filepath_target, sep=target_sep)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Target file not found: {filepath_target}\n"
            f"Run assemble_raw() first to create necessary files."
        )
    
    df_trg,_ = explode_time_components(df_trg, trans_date_label)
    df_trg[trans_process_label] = "IST"
    
    # read input file
    try:
        df_input = pd.read_parquet(join(OUTPUT_DIR, trans_df_input))
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Processed input file not found: {join(OUTPUT_DIR, trans_df_input)}\n"
            f"Run process_raw() first to create this file."
        )
    
    # ============================================================================
    # NORMALIZE INPUT DATA (min-max per variable, on the final clean population)
    #
    # Normalization is intentionally deferred to here (after filter_sparse_columns
    # and filter_by_hsic in the sequential pipeline) so that the normalization
    # statistics reflect the actual surviving parameter set and its data distribution.
    #
    # The denormalization map is saved as denorm_map.json for reproducible
    # inverse-transform at inference time.
    # ============================================================================
    logging.info("Normalizing input data (min-max per variable)...")

    # Compute per-variable min and max
    _norm_stats = (
        df_input.groupby(trans_variable_label)[trans_value_label]
        .agg(["min", "max"])
    )

    # Build denorm map: {variable_name: {min: float, max: float}}
    denorm_map = {
        str(var): {"min": float(row["min"]), "max": float(row["max"])}
        for var, row in _norm_stats.iterrows()
    }

    # Vectorized min-max normalization per variable
    _v_min = df_input[trans_variable_label].map(_norm_stats["min"])
    _v_max = df_input[trans_variable_label].map(_norm_stats["max"])
    _v_range = (_v_max - _v_min).clip(lower=1e-12)  # avoid division by zero
    df_input = df_input.copy()
    df_input[trans_value_norm_label] = (df_input[trans_value_label] - _v_min) / _v_range

    denorm_map_path = join(OUTPUT_DIR, "denorm_map.json")
    with open(denorm_map_path, "w", encoding="utf-8") as _f:
        json.dump(denorm_map, _f, indent=2)
    logging.info(
        f"Normalization complete: {len(denorm_map)} variables normalized. "
        f"Saved denorm_map.json"
    )

    # ============================================================================
    # CREATE VOCABULARY MAPPINGS
    # ============================================================================
    # Maps convert categorical features to integers, saved as JSON dictionaries.
    # Indices start from 1 (0 reserved for missing values in some contexts).
    
    # Group ID mapping (sample identifiers)
    group_array = df_input[trans_group_id].unique()
    group_dict = {group: j for j, group in enumerate(group_array)}
    logging.info(f"Group dictionary size: {len(group_dict)}")
    with open(join(OUTPUT_DIR, group_dict_filename), "w") as f:
        json.dump(group_dict, f, indent=4)
    
    # Process mapping for input (e.g., 1: Laser, 2: Plasma, etc.)
    pro_array_input = df_input[trans_process_label].unique()
    pro_dict_input = {pro: j for j, pro in enumerate(pro_array_input, start=1)}
    logging.info(f"Process dictionary size: {len(pro_dict_input)}")
    with open(join(OUTPUT_DIR, process_dict_filename), "w") as f:
        json.dump(pro_dict_input, f, indent=4)
    
    # ============================================================================
    # SPLIT INPUT BY CLASS (Optional - for CausaliT compatibility)
    # ============================================================================
    # When split_input_by_class=True, split input data into:
    #   - S (Source): Set parameters (class=2) - recipe values set beforehand
    #   - X (Input): Read parameters (class=1) - measured process values
    
    if split_input_by_class:
        # Validate that class column exists and has expected values
        if trans_class_label not in df_input.columns:
            raise ValueError(
                f"Cannot split by class: '{trans_class_label}' column not found in input data.\n"
                f"Make sure assemble_raw() was run with class labeling enabled."
            )
        
        class_values = df_input[trans_class_label].unique()
        logging.info(f"Found class values in input data: {class_values}")
        
        # Split dataframes by class
        # Class 2 = Set parameters -> Source (S)
        # Class 1 = Read parameters -> Input (X)
        df_source = df_input[df_input[trans_class_label] == 2].copy()
        df_input_read = df_input[df_input[trans_class_label] == 1].copy()
        
        # Log split statistics
        n_source_rows = len(df_source)
        n_input_rows = len(df_input_read)
        logging.info(f"Split input by class: {n_source_rows} source rows (Set), {n_input_rows} input rows (Read)")
        
        if n_source_rows == 0:
            logging.warning("No Set parameters (class=2) found - S array will be empty")
        if n_input_rows == 0:
            logging.warning("No Read parameters (class=1) found - X array will be empty")
        
        # Variable mapping for source (Set parameters)
        var_array_source = df_source[trans_variable_label].unique()
        var_dict_source = {var: i for i, var in enumerate(var_array_source, start=1)}
        logging.info(f"Source variables dictionary size: {len(var_dict_source)}")
        with open(join(OUTPUT_DIR, var_dict_filename + "_source"), "w") as f:
            json.dump(var_dict_source, f, indent=4)
        
        # Variable mapping for input (Read parameters)
        var_array_input = df_input_read[trans_variable_label].unique()
        var_dict_input = {var: i for i, var in enumerate(var_array_input, start=1)}
        logging.info(f"Input variables dictionary size: {len(var_dict_input)}")
        with open(join(OUTPUT_DIR, var_dict_filename + "_input"), "w") as f:
            json.dump(var_dict_input, f, indent=4)
        
        # Replace df_input with the Read-only subset for subsequent processing
        df_input = df_input_read
        
    else:
        # Original behavior: Variable mapping for all input (e.g., 1: las_1, ..., 200: pla_35)
        # Note: Proprietary names are available in lookup_selected.xlsx
        var_array_input = df_input[trans_variable_label].unique()
        var_dict_input = {var: i for i, var in enumerate(var_array_input, start=1)}
        logging.info(f"Input variables dictionary size: {len(var_dict_input)}")
        with open(join(OUTPUT_DIR, var_dict_filename + "_input"), "w") as f:
            json.dump(var_dict_input, f, indent=4)
        
        # No source data when not splitting
        df_source = None
        var_dict_source = None
    
    # Variable mapping for target (e.g., 1: Sense_A, 2: Sense_B)
    # Note: occurrence and step features are already integers (handled by process_raw.py)
    var_array_trg = df_trg[trans_variable_label].unique()
    if var_array_trg.dtype == "int64":
        # Convert int64 to string for consistent mapping
        var_array_trg = var_array_trg.astype(str)
        df_trg[trans_variable_label] = df_trg[trans_variable_label].astype(str)
    
    var_dict_trg = {str(var): i for i, var in enumerate(var_array_trg, start=1)}
    logging.info(f"Target variables dictionary size: {len(var_dict_trg)}")
    with open(join(OUTPUT_DIR, var_dict_filename + "_trg"), "w") as f:
        json.dump(var_dict_trg, f, indent=4)
    
    # ============================================================================
    # APPLY MAPPINGS TO DATAFRAMES
    # ============================================================================
    df_input[trans_process_label]  = df_input[trans_process_label].map(pro_dict_input)
    df_trg[trans_variable_label]   = df_trg[trans_variable_label].map(var_dict_trg)
    df_input[trans_variable_label] = df_input[trans_variable_label].map(var_dict_input)
    df_input[trans_group_id] = df_input[trans_group_id].map(group_dict)
    df_trg[trans_group_id] = df_trg[trans_group_id].map(group_dict)
    
    # Apply mappings to source dataframe if splitting
    if split_input_by_class and df_source is not None:
        df_source[trans_process_label] = df_source[trans_process_label].map(pro_dict_input)
        df_source[trans_variable_label] = df_source[trans_variable_label].map(var_dict_source)
        df_source[trans_group_id] = df_source[trans_group_id].map(group_dict)
    
    # Get unique sample IDs from the mapped group column
    # Use df_source if available (contains all samples), otherwise use df_input
    if split_input_by_class and df_source is not None and len(df_source) > 0:
        # Get samples from both source and input to ensure we capture all
        id_samples_source = set(df_source[trans_group_id].unique())
        id_samples_input = set(df_input[trans_group_id].unique())
        id_samples = np.array(list(id_samples_source.union(id_samples_input)))
    else:
        id_samples = df_input[trans_group_id].unique()
    
    # ============================================================================
    # COMPUTE SAMPLE METRICS
    # ============================================================================
    # Calculate metrics for stratified train/test splitting
    rarity_df = apply_metrics([
        (compute_rarity_last_value, {
            'df': df_trg,
            'group_id_label': trans_group_id,
            'variable_label': trans_variable_label,
            'value_label': trans_value_label,
            'position_label': trans_position_label,
            'n_bins': 50
        }),
        (compute_rarity_nan_fraction, {
            'df': df_input,
            'group_id_label': trans_group_id,
            'value_label': trans_value_norm_label,
            'n_bins': 50,
            'tag' : 'input'
        }),
        (compute_rarity_nan_fraction, {
            'df': df_trg,
            'group_id_label': trans_group_id,
            'value_label': trans_value_label,
            'n_bins': 50,
            'tag' : 'target'
        })
    ])
    
    # Save metrics for later use in stratified splitting
    rarity_df.to_parquet(join(OUTPUT_DIR, "sample_metrics.parquet"))
    logging.info("Saved sample metrics to sample_metrics.parquet")
    
    # ============================================================================
    # DEFINE FEATURE SCHEMA
    # ============================================================================
    # Feature indices for X (input) array:
    #   0: group_id    - sample ID
    #   1: process     - process ID
    #   2: occurrence  - occurrence of the same process
    #   3: step        - step within same process occurrence
    #   4: variable    - parameter ID
    #   5: value       - normalized values
    #   6: order       - causal order from timestamps
    #   7-11: time     - time components (year, month, day, hour, minute)
    #   12: class      - parameter class (1=Read, 2=Set)
    #
    # Feature indices for Y (target) array:
    #   0: group_id    - sample ID
    #   1: position    - position (for IST, cycle number)
    #   2: variable    - parameter ID (for IST: Sense_A, Sense_B)
    #   3: value       - real values
    #   4+: time       - time components (year, month, day, hour, minute)
    
    input_features = [
        trans_process_label,
        trans_occurrence_label,
        trans_step_label, 
        trans_variable_label, 
        trans_value_norm_label,
        trans_order_label
        ]
    input_features.extend(time_components_labels)
    
    # Only append class feature if NOT splitting by class (redundant when split)
    if not split_input_by_class:
        input_features.append(trans_class_label)  # Class feature (Read/Set) appended at end for backward compatibility
    
    trg_features = [
        trans_position_label,
        trans_variable_label, 
        trans_value_label
    ]
    trg_features.extend(time_components_labels)
    
    # Assemble feature dictionary (documents feature-to-index mappings for model design)
    feat_dict = {
        "input": {key + 1: val for key, val in enumerate(input_features)},
        "target": {key + 1: val for key, val in enumerate(trg_features)}
    }
    
    # Assign group_id at index 0 for both input and target
    feat_dict["input"][0] = trans_group_id
    feat_dict["target"][0] = trans_group_id
    
    # Add source features if splitting (same schema as input)
    if split_input_by_class:
        feat_dict["source"] = {key + 1: val for key, val in enumerate(input_features)}
        feat_dict["source"][0] = trans_group_id
        feat_dict["source"] = {key: feat_dict["source"][key] for key in sorted(feat_dict["source"].keys())}
    
    # Sort for better readability
    feat_dict["input"] = {key: feat_dict["input"][key] for key in sorted(feat_dict["input"].keys())}
    feat_dict["target"] = {key: feat_dict["target"][key] for key in sorted(feat_dict["target"].keys())}
    
    with open(join(OUTPUT_DIR, feat_dict_filename), "w") as f:
        json.dump(feat_dict, f, indent=4)
    
    # ============================================================================
    # CONVERT TO NUMPY ARRAYS
    # ============================================================================
    logging.info("Flattening input sequence to numpy array")
    array_input = pandas_to_numpy_ds(id_samples, df_input, input_features, trans_group_id, 2000)
    
    logging.info("Flattening target sequence to numpy array")
    array_trg = pandas_to_numpy_ds(id_samples, df_trg, trg_features, trans_group_id, 3000)
    
    # Generate source array if splitting by class
    if split_input_by_class and df_source is not None and len(df_source) > 0:
        logging.info("Flattening source sequence to numpy array")
        array_source = pandas_to_numpy_ds(id_samples, df_source, input_features, trans_group_id, 2000)
    else:
        array_source = None
    
    # ============================================================================
    # SAVE DATASET
    # ============================================================================
    dataset_name = f"ds_{dataset_id}"
    dataset_dir = join(OUTPUT_DIR, dataset_name)
    if not exists(dataset_dir):
        mkdir(dataset_dir)
    
    # Save as compressed numpy archive (npz format)
    if split_input_by_class and array_source is not None:
        # CausaliT-compatible format: S (source), X (input), Y (target)
        np.savez(join(dataset_dir, full_data_label), s=array_source, x=array_input, y=array_trg)
        logging.info(f"Saved compressed dataset to {full_data_label} (S: {array_source.shape}, X: {array_input.shape}, Y: {array_trg.shape})")
    else:
        # Original format: X (input), Y (target)
        np.savez(join(dataset_dir, full_data_label), x=array_input, y=array_trg)
        logging.info(f"Saved compressed dataset to {full_data_label} (X: {array_input.shape}, Y: {array_trg.shape})")
    
    
if __name__ == "__main__":
    generate_dataset(dataset_id = "dyconex_251117")
