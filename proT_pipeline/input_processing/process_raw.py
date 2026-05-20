import pandas as pd
import numpy as np
from os.path import join
import json
from proT_pipeline.labels import *
from proT_pipeline.core.modules import explode_time_components
from proT_pipeline.utils import safe_read_csv



def process_raw(dataset_id: str, missing_threshold: float = None)->None:
    """
    Process raw process data. Operations are:
    - Take the mean of multiple measurements
    - Add temporal order column
    - Explode time components
    - Assigns occurrence and steps

    Normalization and column-level missing-value filtering are intentionally
    NOT performed here.  They are handled downstream:
    - Column filtering: ``filter_sparse_columns()`` (shared by both modes)
    - Normalization: inside ``generate_dataset()`` (sequential) or
      ``generate_tabular_dataset()`` (tabular), AFTER all filtering is done,
      so that the normalization statistics reflect the final clean population.

    The ``missing_threshold`` parameter is retained for backward compatibility
    but is no longer used (a deprecation warning is logged if it is passed).

    Args:
        dataset_id (str): working directory
        missing_threshold (float): DEPRECATED — use filter_sparse_columns() instead
    """
    import warnings
    if missing_threshold is not None:
        warnings.warn(
            "process_raw: the 'missing_threshold' parameter is deprecated and has no effect. "
            "Use filter_sparse_columns(dataset_id, col_missing_threshold=...) after process_raw() "
            "to filter sparse columns.",
            DeprecationWarning,
            stacklevel=2,
        )
    
    # ============================================================================
    # SETUP
    # ============================================================================
    ROOT_DIR = get_root_dir()
    _, OUTPUT_DIR, CONTROL_DIR = get_dirs(ROOT_DIR, dataset_id)

    # Load raw process data
    try:
        df_raw = safe_read_csv(join(OUTPUT_DIR, trans_df_process_raw))
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Raw process file not found: {join(OUTPUT_DIR, trans_df_process_raw)}\n"
            f"Run assemble_raw() first to create this file."
        )

    # ============================================================================
    # AGGREGATE
    # Normalization is NOT done here — it is deferred to the mode-specific
    # downstream steps (generate_dataset / generate_tabular_dataset) so that
    # normalization statistics are computed on the final clean population.
    # ============================================================================

    # Aggregate multiple measurements by taking mean
    # Note: trans_class_label is included in grouping to preserve class information
    grouping_cols = [trans_design_version_label, trans_group_id, trans_position_label, trans_process_label, trans_variable_label, trans_class_label]
    df_processed = df_raw.groupby(grouping_cols).agg({
        trans_value_label: "mean",
        trans_date_label: "first",
        trans_parameter_label: "first"
        }).reset_index()

    # Add temporal ordering based on timestamps
    df_processed[trans_order_label] = (
        df_processed.groupby(trans_group_id)[trans_date_label].rank(
            method='dense',  
            ascending=True,          
            na_option='keep'
        ).astype('Int64'))
    
    df_processed[trans_order_label] = df_processed[trans_order_label].astype('float64')

    # Extract time components from timestamps
    df_processed, _ = explode_time_components(df_processed, trans_date_label)

    # ============================================================================
    # ASSIGN OCCURRENCE
    # ============================================================================
    # Load layer information from process control file
    excel_file = pd.read_excel(join(CONTROL_DIR, "Prozessfolgen_MSEI.xlsx"), skiprows=1, sheet_name=None)
    
    _design_version_position_label = "design_version_position"
    _layer_label = "layer"
    
    # Combine all sheets into single dataframe
    df_layer = pd.concat(
        [
        sheet_df.assign(sheet_name=sheet_name) for sheet_name, sheet_df in excel_file.items()
        ], ignore_index=True)
    df_layer[_design_version_position_label] = df_layer["sheet_name"].astype(str) + "_" + df_layer[occurrence_position_label].astype(str)
    
    # Create mapping from design-version-position to layer
    des_ver_pos_to_layer_map = df_layer.set_index(_design_version_position_label)[occurrence_layer_label].to_dict()
    
    df_processed[_design_version_position_label] = df_processed[trans_design_version_label].astype(str) + "_" + df_processed[trans_position_label].astype(int).astype(str)
    df_processed[_layer_label] = df_processed[_design_version_position_label].map(des_ver_pos_to_layer_map)
    
    # Map layers to occurrence numbers
    layer_to_occurrence_map = {
        "3,4": 1,
        "2,5": 2,
        "1,6": 3,
        "Endoberfläche": 4
    }
    
    df_processed[trans_occurrence_label] = df_processed[_layer_label].map(layer_to_occurrence_map)    
    
    # ============================================================================
    # ASSIGN STEPS
    # ============================================================================
    steps_dict = {}
    for pro in df_processed[trans_process_label].unique():
        for occ in df_processed.set_index(trans_process_label).loc[pro, trans_occurrence_label].unique():
            mask = np.logical_and(df_processed[trans_process_label] == pro, df_processed[trans_occurrence_label] == occ)
            steps = df_processed.set_index([trans_process_label, trans_occurrence_label]).loc[pro].loc[occ][trans_position_label].unique()
            steps_map = {step: i + 1 for i, step in enumerate(steps)}
            steps_dict[str(pro) + "_" + str(occ)] = steps_map
            df_processed.loc[mask, trans_step_label] = df_processed.loc[mask, trans_position_label].map(steps_map)

    with open(join(OUTPUT_DIR, step_dict_filename), "w") as f:
        json.dump(steps_dict, f, indent=4)
    
    # ============================================================================
    # SAVE OUTPUT
    # ============================================================================
    df_processed.to_parquet(join(OUTPUT_DIR, trans_df_input))
    
    
if __name__ == "__main__":
    process_raw(dataset_id = "dx_occurrence_test")
