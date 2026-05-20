"""
HSIC-based parameter filtering for feature selection.

This module filters input parameters based on their statistical dependence
with the target variable using HSIC (Hilbert-Schmidt Independence Criterion).

Parameters with HSIC > k * baseline are retained, where:
- baseline is the maximum HSIC from random baseline signals
- k is a threshold multiplier (default 1.0)

This filtering step should be run AFTER process_raw() and BEFORE generate_dataset().

Usage:
    from proT_pipeline.input_processing.filter_by_hsic import filter_by_hsic
    filter_info = filter_by_hsic(dataset_id="my_dataset", threshold_multiplier=1.0)
"""

import numpy as np
import pandas as pd
import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Tuple
from os.path import join, exists

import sys
from os.path import dirname, abspath
ROOT_DIR = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(ROOT_DIR)

from proT_pipeline.labels import (
    get_root_dir, get_dirs,
    trans_group_id, trans_process_label, trans_occurrence_label,
    trans_step_label, trans_variable_label, trans_value_norm_label,
    trans_value_label,
    trans_df_input, target_filename, target_sep
)
from proT_pipeline.utils import safe_read_csv

# Import HSIC functions from data_analysis module
from data_analysis.hsic import (
    run_hsic_analysis,
    compute_slopes,
    pivot_slopes,
    extract_unique_params,
    compute_baseline_hsic,
    compute_hsic_for_all_params
)


def get_baseline_threshold(
    baseline_df: pd.DataFrame,
    threshold_multiplier: float = 1.0
) -> Tuple[str, float, float]:
    """
    Get the baseline threshold for HSIC filtering.
    
    Selects the baseline with highest HSIC score and applies multiplier.
    
    Parameters
    ----------
    baseline_df : pd.DataFrame
        DataFrame with baseline HSIC results (from compute_baseline_hsic)
    threshold_multiplier : float
        Multiplier k for the threshold (default 1.0)
    
    Returns
    -------
    tuple
        (baseline_name, baseline_value, threshold_value)
    """
    # Find baseline with maximum HSIC (excluding NaN)
    valid_baselines = baseline_df[baseline_df['hsic_score'].notna()]
    
    if len(valid_baselines) == 0:
        raise ValueError("No valid baseline HSIC scores found")
    
    max_idx = valid_baselines['hsic_score'].idxmax()
    baseline_row = valid_baselines.loc[max_idx]
    
    baseline_name = baseline_row['baseline_name']
    baseline_value = baseline_row['hsic_score']
    threshold = threshold_multiplier * baseline_value
    
    return baseline_name, baseline_value, threshold


def filter_by_hsic(
    dataset_id: str,
    target_sense: str = 'A',
    threshold_multiplier: float = 1.0,
    aggregation: str = 'mean'
) -> Dict[str, Any]:
    """
    Filter parameters based on HSIC independence criterion.
    
    Keeps parameters where HSIC > k * max(baseline_HSIC).
    
    This function:
    1. Runs HSIC analysis if not already cached
    2. Determines threshold from baseline HSIC scores
    3. Filters df_input.parquet to keep only significant parameters
    4. Saves filtered data back to df_input.parquet
    5. Returns detailed filtering information for logging
    
    Parameters
    ----------
    dataset_id : str
        Dataset identifier (folder name in data/builds/)
    target_sense : str
        Target variable to use for independence testing.
        Default 'A' (Sense_A). Options depend on target variables.
    threshold_multiplier : float
        Multiplier k for the threshold. Parameters with HSIC > k * baseline
        are retained. Default 1.0 (keep params with HSIC above highest baseline).
    aggregation : str
        Aggregation method for HSIC computation: 'mean', 'first', 'last'
    
    Returns
    -------
    dict
        Dictionary containing:
        - 'baseline_name': Name of baseline used (e.g., 'baseline_random_uniform')
        - 'baseline_value': HSIC score of the baseline
        - 'threshold_multiplier': The k value used
        - 'threshold': Computed threshold (k * baseline_value)
        - 'n_params_before': Number of parameters before filtering
        - 'n_params_after': Number of parameters after filtering
        - 'n_params_filtered': Number of parameters removed
        - 'params_filtered': List of dicts with filtered parameter details
        - 'params_retained': List of retained parameter names
        - 'unique_vars_filtered': Unique variable names that were filtered out
    
    Raises
    ------
    FileNotFoundError
        If required input files are missing
    ValueError
        If target sense not found in HSIC results
    
    Notes
    -----
    - Modifies df_input.parquet in place
    - Creates a backup at df_input_unfiltered.parquet before filtering
    - HSIC analysis results are cached and reused if available
    
    Examples
    --------
    >>> from proT_pipeline.input_processing.filter_by_hsic import filter_by_hsic
    >>> info = filter_by_hsic(dataset_id="my_dataset", threshold_multiplier=1.0)
    >>> print(f"Filtered {info['n_params_filtered']} parameters")
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting HSIC-based parameter filtering for dataset: {dataset_id}")
    logger.info(f"Target sense: {target_sense}, Threshold multiplier (k): {threshold_multiplier}")
    
    # =========================================================================
    # SETUP
    # =========================================================================
    ROOT_DIR = get_root_dir()
    INPUT_DIR, OUTPUT_DIR, CONTROL_DIR = get_dirs(ROOT_DIR, dataset_id)
    
    df_input_path = join(OUTPUT_DIR, trans_df_input)
    df_input_backup_path = join(OUTPUT_DIR, "df_input_unfiltered.parquet")
    
    if not exists(df_input_path):
        raise FileNotFoundError(
            f"Processed input file not found: {df_input_path}\n"
            f"Run process_raw() first to create this file."
        )
    
    # =========================================================================
    # RUN HSIC ANALYSIS
    # =========================================================================
    logger.info("Running HSIC analysis (or loading from cache)...")
    hsic_results = run_hsic_analysis(dataset_id=dataset_id, aggregation=aggregation)
    
    # Find the correct key for the target sense
    # Keys might be like 'hsic_A', 'hsic_B', 'hsic_delta_A_norm', etc.
    hsic_keys = [k for k in hsic_results.keys() if k.startswith('hsic_')]
    baseline_keys = [k for k in hsic_results.keys() if k.startswith('baseline_')]
    
    # Try to find matching sense
    hsic_key = None
    baseline_key = None
    
    for k in hsic_keys:
        if target_sense in k:
            hsic_key = k
            break
    
    for k in baseline_keys:
        if target_sense in k:
            baseline_key = k
            break
    
    if hsic_key is None:
        available = [k.replace('hsic_', '') for k in hsic_keys]
        raise ValueError(
            f"Target sense '{target_sense}' not found in HSIC results.\n"
            f"Available senses: {available}"
        )
    
    if baseline_key is None:
        # Fallback: use first baseline
        baseline_key = baseline_keys[0] if baseline_keys else None
        logger.warning(f"Baseline for sense '{target_sense}' not found, using: {baseline_key}")
    
    logger.info(f"Using HSIC results: {hsic_key}")
    logger.info(f"Using baseline: {baseline_key}")
    
    hsic_df = hsic_results[hsic_key]
    baseline_df = hsic_results[baseline_key]
    
    # =========================================================================
    # COMPUTE THRESHOLD
    # =========================================================================
    baseline_name, baseline_value, threshold = get_baseline_threshold(
        baseline_df, threshold_multiplier
    )
    
    logger.info(f"Baseline: {baseline_name} (HSIC = {baseline_value:.6f})")
    logger.info(f"Threshold = {threshold_multiplier} × {baseline_value:.6f} = {threshold:.6f}")
    
    # =========================================================================
    # IDENTIFY PARAMETERS TO FILTER
    # =========================================================================
    # Filter out params with hsic_score = 1.0 (perfect correlation, likely duplicates)
    # and params with HSIC <= threshold
    hsic_df_valid = hsic_df[hsic_df['hsic_score'] < 1.0].copy()
    
    params_above_threshold = hsic_df_valid[hsic_df_valid['hsic_score'] > threshold]
    params_below_threshold = hsic_df_valid[hsic_df_valid['hsic_score'] <= threshold]
    
    n_params_before = len(hsic_df_valid)
    n_params_after = len(params_above_threshold)
    n_params_filtered = len(params_below_threshold)
    
    logger.info(f"Parameters before filtering: {n_params_before}")
    logger.info(f"Parameters after filtering: {n_params_after}")
    logger.info(f"Parameters filtered out: {n_params_filtered}")
    
    # =========================================================================
    # PREPARE FILTERING INFO FOR LOGGING
    # =========================================================================
    # Get details of filtered parameters
    params_filtered_list = []
    for _, row in params_below_threshold.iterrows():
        params_filtered_list.append({
            'param_name': row['param_name'],
            'process': row['process'],
            'occurrence': row['occurrence'],
            'step': row['step'],
            'variable': row['variable'],
            'hsic_score': row['hsic_score']
        })
    
    # Sort by HSIC score (lowest first)
    params_filtered_list = sorted(params_filtered_list, key=lambda x: x['hsic_score'])
    
    # Get unique variables that were filtered out
    unique_vars_filtered = params_below_threshold['variable'].unique().tolist()
    
    # Parameters retained
    params_retained = params_above_threshold['param_name'].tolist()
    
    # =========================================================================
    # FILTER df_input.parquet
    # =========================================================================
    logger.info("Loading df_input.parquet for filtering...")
    df_input = pd.read_parquet(df_input_path)
    
    # Save backup before filtering
    logger.info(f"Saving backup to {df_input_backup_path}")
    df_input.to_parquet(df_input_backup_path)
    
    # Create param_name column for filtering.
    # Use integer conversion for occurrence and step so the key matches the
    # format produced by _make_col_label() in generate_tabular_dataset.py.
    def _int_str(x):
        return str(int(x)) if pd.notna(x) else "NA"

    df_input['_param_name'] = (
        df_input[trans_process_label].astype(str) + '_' +
        df_input[trans_occurrence_label].apply(_int_str) + '_' +
        df_input[trans_step_label].apply(_int_str) + '_' +
        df_input[trans_variable_label].astype(str)
    )
    
    # Filter to keep only retained parameters
    df_filtered = df_input[df_input['_param_name'].isin(params_retained)].copy()
    df_filtered = df_filtered.drop(columns=['_param_name'])
    
    # Log filtering results
    rows_before = len(df_input)
    rows_after = len(df_filtered)
    logger.info(f"Rows before filtering: {rows_before}")
    logger.info(f"Rows after filtering: {rows_after}")
    
    # Save filtered data
    logger.info(f"Saving filtered data to {df_input_path}")
    df_filtered.to_parquet(df_input_path)
    
    # =========================================================================
    # LOG FILTERED PARAMETERS
    # =========================================================================
    logger.info("\n" + "="*60)
    logger.info("HSIC FILTERING SUMMARY")
    logger.info("="*60)
    logger.info(f"Baseline used: {baseline_name}")
    logger.info(f"Baseline HSIC value: {baseline_value:.6f}")
    logger.info(f"Threshold multiplier (k): {threshold_multiplier}")
    logger.info(f"Threshold: {threshold:.6f}")
    logger.info(f"Parameters filtered out: {n_params_filtered}/{n_params_before}")
    
    if n_params_filtered > 0:
        logger.info("\nFiltered parameters (sorted by HSIC, ascending):")
        for param_info in params_filtered_list[:20]:  # Log first 20
            logger.info(
                f"  {param_info['param_name']}: "
                f"HSIC={param_info['hsic_score']:.4f}, "
                f"process={param_info['process']}, "
                f"var={param_info['variable']}"
            )
        if len(params_filtered_list) > 20:
            logger.info(f"  ... and {len(params_filtered_list) - 20} more")
        
        logger.info(f"\nUnique variables filtered out ({len(unique_vars_filtered)}):")
        for var in unique_vars_filtered[:30]:  # Log first 30
            logger.info(f"  - {var}")
        if len(unique_vars_filtered) > 30:
            logger.info(f"  ... and {len(unique_vars_filtered) - 30} more")
    
    logger.info("="*60)
    
    # =========================================================================
    # PREPARE FILTER INFO
    # =========================================================================
    filter_info = {
        'baseline_name': baseline_name,
        'baseline_value': baseline_value,
        'threshold_multiplier': threshold_multiplier,
        'threshold': threshold,
        'n_params_before': n_params_before,
        'n_params_after': n_params_after,
        'n_params_filtered': n_params_filtered,
        'params_filtered': params_filtered_list,
        'params_retained': params_retained,
        'unique_vars_filtered': unique_vars_filtered,
        'rows_before': rows_before,
        'rows_after': rows_after
    }
    
    # =========================================================================
    # SAVE FILTERING SUMMARY TO FILE
    # =========================================================================
    hsic_analysis_dir = join(OUTPUT_DIR, "hsic_analysis")
    filter_summary_path = join(hsic_analysis_dir, "hsic_filter_summary.json")
    
    # Prepare JSON-serializable summary
    filter_summary = {
        'timestamp': datetime.now().isoformat(),
        'dataset_id': dataset_id,
        'target_sense': target_sense,
        'hsic_key_used': hsic_key,
        'baseline_key_used': baseline_key,
        'baseline_name': baseline_name,
        'baseline_value': float(baseline_value),
        'threshold_multiplier': float(threshold_multiplier),
        'threshold': float(threshold),
        'n_params_before': int(n_params_before),
        'n_params_after': int(n_params_after),
        'n_params_filtered': int(n_params_filtered),
        'rows_before': int(rows_before),
        'rows_after': int(rows_after),
        'unique_vars_filtered': unique_vars_filtered,
        'params_filtered': [
            {
                'param_name': p['param_name'],
                'process': str(p['process']),
                'occurrence': int(p['occurrence']) if pd.notna(p['occurrence']) else None,
                'step': int(p['step']) if pd.notna(p['step']) else None,
                'variable': str(p['variable']),
                'hsic_score': float(p['hsic_score']) if pd.notna(p['hsic_score']) else None
            }
            for p in params_filtered_list
        ]
    }
    
    with open(filter_summary_path, 'w') as f:
        json.dump(filter_summary, f, indent=2)
    
    logger.info(f"Saved filtering summary to {filter_summary_path}")
    
    # Also save filtered parameters as CSV for easier inspection
    if n_params_filtered > 0:
        filtered_params_csv_path = join(hsic_analysis_dir, "hsic_filtered_params.csv")
        params_below_threshold.to_csv(filtered_params_csv_path, index=False)
        logger.info(f"Saved filtered parameters list to {filtered_params_csv_path}")
    
    return filter_info


# ============================================================================
# DATAFRAME-LEVEL API (for per-partition tabular use)
# ============================================================================

def filter_dataframe_by_hsic(
    df_input: pd.DataFrame,
    target_series: pd.Series,
    threshold_multiplier: float = 1.0,
    aggregation: str = "mean",
    use_raw_values: bool = True,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Filter a long-format input DataFrame by HSIC independence criterion.

    This is a DataFrame-level alternative to ``filter_by_hsic()`` that does NOT
    require a ``dataset_id`` or file I/O, making it suitable for per-partition
    filtering inside ``generate_tabular_dataset()``.

    Parameters
    ----------
    df_input : pd.DataFrame
        Long-format process input data (from ``df_input.parquet``).
        Must contain ``trans_group_id``, ``trans_process_label``,
        ``trans_occurrence_label``, ``trans_step_label``,
        ``trans_variable_label``, and either ``trans_value_label`` (raw)
        or ``trans_value_norm_label`` (normalized).
    target_series : pd.Series
        Scalar target value per group, indexed by ``trans_group_id``.
        Typically a slope column from ``slopes_summary.csv``.
    threshold_multiplier : float
        Multiplier k for the HSIC threshold.  Parameters with
        HSIC > k × max(baseline_HSIC) are retained.  Default 1.0.
    aggregation : str
        Aggregation method when multiple rows share the same
        (group, parameter) combination.  ``'mean'`` (default), ``'first'``,
        or ``'last'``.
    use_raw_values : bool
        If ``True`` (default), pivot using ``trans_value_label`` (raw values).
        HSIC with RBF kernel + median-heuristic bandwidth is scale-invariant,
        so raw values are statistically equivalent to normalized values.
        Set ``False`` to use pre-computed ``trans_value_norm_label`` if present.

    Returns
    -------
    df_filtered : pd.DataFrame
        Filtered long-format DataFrame (same columns as input, subset of rows).
    filter_info : dict
        Same keys as returned by ``filter_by_hsic()``:
        ``baseline_name``, ``baseline_value``, ``threshold_multiplier``,
        ``threshold``, ``n_params_before``, ``n_params_after``,
        ``n_params_filtered``, ``params_filtered``, ``params_retained``,
        ``unique_vars_filtered``, ``rows_before``, ``rows_after``.
    """
    logger = logging.getLogger(__name__)

    value_col = trans_value_label if use_raw_values else trans_value_norm_label

    # ------------------------------------------------------------------
    # Pivot to wide format to get X_params and Y aligned
    # ------------------------------------------------------------------
    X_params, param_info = extract_unique_params(
        df_input,
        value_label=value_col,
        aggregation=aggregation,
    )

    # Align target to the groups present in X_params
    common_groups = X_params.index.intersection(target_series.index)
    if len(common_groups) == 0:
        logger.warning(
            "[filter_dataframe_by_hsic] No groups in common between df_input "
            "and target_series — returning df_input unfiltered."
        )
        return df_input.copy(), {
            "baseline_name": None, "baseline_value": None,
            "threshold_multiplier": threshold_multiplier, "threshold": None,
            "n_params_before": 0, "n_params_after": 0, "n_params_filtered": 0,
            "params_filtered": [], "params_retained": [],
            "unique_vars_filtered": [],
            "rows_before": len(df_input), "rows_after": len(df_input),
        }

    X_sub = X_params.loc[common_groups]
    Y_vals = target_series.loc[common_groups].values.astype(float)

    # ------------------------------------------------------------------
    # Baseline + param HSIC
    # ------------------------------------------------------------------
    baseline_df = compute_baseline_hsic(Y_vals, n_samples=len(Y_vals))
    hsic_df = compute_hsic_for_all_params(
        X_params=X_sub,
        Y_slope=Y_vals,
        param_info=param_info,
    )

    # ------------------------------------------------------------------
    # Compute threshold and split params
    # ------------------------------------------------------------------
    baseline_name, baseline_value, threshold = get_baseline_threshold(
        baseline_df, threshold_multiplier
    )
    logger.info(
        f"[filter_dataframe_by_hsic] Baseline={baseline_name} "
        f"(HSIC={baseline_value:.6f}) | threshold={threshold:.6f}"
    )

    hsic_df_valid = hsic_df[hsic_df["hsic_score"] < 1.0].copy()
    params_above = hsic_df_valid[hsic_df_valid["hsic_score"] > threshold]
    params_below = hsic_df_valid[hsic_df_valid["hsic_score"] <= threshold]

    params_retained = params_above["param_name"].tolist()
    params_filtered_list = sorted(
        [
            {
                "param_name": r["param_name"],
                "process": r["process"],
                "occurrence": r.get("occurrence"),
                "step": r.get("step"),
                "variable": r["variable"],
                "hsic_score": r["hsic_score"],
            }
            for _, r in params_below.iterrows()
        ],
        key=lambda x: x["hsic_score"],
    )

    logger.info(
        f"[filter_dataframe_by_hsic] Retained {len(params_retained)}/{len(hsic_df_valid)} "
        f"params ({len(params_filtered_list)} filtered out)"
    )

    # ------------------------------------------------------------------
    # Filter the long-format df_input
    # ------------------------------------------------------------------
    df_input = df_input.copy()
    # Use the same integer-converted format as _make_col_label() so that the
    # keys in params_retained match the _col_label values built from df_long.
    def _int_str(x):
        return str(int(x)) if pd.notna(x) else "NA"

    df_input["_param_name"] = (
        df_input[trans_process_label].astype(str) + "_"
        + df_input[trans_occurrence_label].apply(_int_str) + "_"
        + df_input[trans_step_label].apply(_int_str) + "_"
        + df_input[trans_variable_label].astype(str)
    )
    df_filtered = df_input[df_input["_param_name"].isin(params_retained)].copy()
    df_filtered = df_filtered.drop(columns=["_param_name"])

    filter_info: Dict[str, Any] = {
        "baseline_name": baseline_name,
        "baseline_value": float(baseline_value),
        "threshold_multiplier": threshold_multiplier,
        "threshold": float(threshold),
        "n_params_before": len(hsic_df_valid),
        "n_params_after": len(params_above),
        "n_params_filtered": len(params_below),
        "params_filtered": params_filtered_list,
        "params_retained": params_retained,
        "unique_vars_filtered": params_below["variable"].unique().tolist(),
        "rows_before": len(df_input),
        "rows_after": len(df_filtered),
    }

    return df_filtered, filter_info


if __name__ == "__main__":
    # Example usage
    import sys
    
    # Setup logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    if len(sys.argv) > 1:
        dataset_id = sys.argv[1]
    else:
        dataset_id = "dyconex_test_params_class"  # Default dataset
    
    k = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    
    filter_info = filter_by_hsic(
        dataset_id=dataset_id,
        threshold_multiplier=k
    )
    
    print(f"\nFiltering complete. Removed {filter_info['n_params_filtered']} parameters.")
