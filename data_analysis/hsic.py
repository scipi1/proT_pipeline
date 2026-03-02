"""
HSIC (Hilbert-Schmidt Independence Criterion) analysis for feature selection.

This module provides tools to compute HSIC between input process parameters
and target slopes, enabling feature selection based on statistical dependence.

The workflow:
1. Load processed df_input.parquet and df_trg.csv
2. Compute slopes (Sense_A, Sense_B separately) via linear regression (intercept=0)
3. Extract unique X_params: pivot input data to (process, occurrence, step, variable)
4. Compute HSIC for each X_param vs Y_slope using RBF kernel
5. Rank and save results

Usage:
    from data_analysis.hsic import run_hsic_analysis
    results = run_hsic_analysis(dataset_id="my_dataset")
"""

import numpy as np
import pandas as pd
import logging
from typing import Tuple, Dict, Optional, List
from os.path import join, exists
from os import makedirs
from sklearn.linear_model import LinearRegression

import sys
from os.path import dirname, abspath
ROOT_DIR = dirname(dirname(abspath(__file__)))
sys.path.append(ROOT_DIR)

from proT_pipeline.labels import (
    get_root_dir, get_dirs,
    trans_group_id, trans_process_label, trans_occurrence_label,
    trans_step_label, trans_variable_label, trans_value_norm_label,
    trans_position_label, trans_value_label, trans_class_label,
    trans_df_input, target_filename, target_sep
)
from proT_pipeline.utils import safe_read_csv


# =============================================================================
# SLOPE COMPUTATION
# =============================================================================

def compute_slopes(
    df_trg: pd.DataFrame,
    group_id_label: str = trans_group_id,
    position_label: str = trans_position_label,
    variable_label: str = trans_variable_label,
    value_label: str = trans_value_label
) -> pd.DataFrame:
    """
    Compute linear regression slopes (with intercept=0) for each sample and variable.
    
    For each (group_id, variable) combination, fits a linear model:
        value = slope * position
    
    Parameters
    ----------
    df_trg : pd.DataFrame
        Target dataframe with time-series data
    group_id_label : str
        Column name for sample/group identifier
    position_label : str
        Column name for position/cycle number (X in regression)
    variable_label : str
        Column name for variable identifier (e.g., 'A', 'B' for Sense_A, Sense_B)
    value_label : str
        Column name for the target values (Y in regression)
    
    Returns
    -------
    pd.DataFrame
        DataFrame with columns:
        - group_id: Sample identifier
        - variable: Variable name (A or B)
        - slope: Regression slope
        - r2: Coefficient of determination (R²)
    
    Notes
    -----
    - Uses LinearRegression with fit_intercept=False
    - Samples/variables with fewer than 2 valid points are skipped
    - R² is computed as 1 - SS_res/SS_tot
    """
    logger = logging.getLogger(__name__)
    
    results = []
    sample_ids = df_trg[group_id_label].unique()
    variables = df_trg[variable_label].unique()
    
    logger.info(f"Computing slopes for {len(sample_ids)} samples, {len(variables)} variables")
    
    skipped_count = 0
    
    for sample_id in sample_ids:
        sample_df = df_trg[df_trg[group_id_label] == sample_id]
        
        for var in variables:
            var_df = sample_df[sample_df[variable_label] == var].copy()
            
            # Drop NaN values
            var_df = var_df.dropna(subset=[position_label, value_label])
            
            if len(var_df) < 2:
                skipped_count += 1
                continue
            
            # Prepare data for regression
            X = var_df[position_label].values.reshape(-1, 1)
            y = var_df[value_label].values
            
            # Fit linear regression with intercept=0
            model = LinearRegression(fit_intercept=False)
            model.fit(X, y)
            
            slope = model.coef_[0]
            
            # Compute R² manually (since fit_intercept=False affects sklearn's r2)
            y_pred = model.predict(X)
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
            
            results.append({
                group_id_label: sample_id,
                variable_label: var,
                'slope': slope,
                'r2': r2
            })
    
    if skipped_count > 0:
        logger.warning(f"Skipped {skipped_count} (sample, variable) pairs with < 2 valid points")
    
    result_df = pd.DataFrame(results)
    logger.info(f"Computed slopes for {len(result_df)} (sample, variable) pairs")
    
    return result_df


def pivot_slopes(
    slopes_df: pd.DataFrame,
    group_id_label: str = trans_group_id,
    variable_label: str = trans_variable_label
) -> pd.DataFrame:
    """
    Pivot slopes DataFrame to wide format with separate columns per variable.
    
    Parameters
    ----------
    slopes_df : pd.DataFrame
        Output from compute_slopes()
    group_id_label : str
        Column name for sample identifier
    variable_label : str
        Column name for variable identifier
    
    Returns
    -------
    pd.DataFrame
        Wide format with columns: [group_id, slope_A, r2_A, slope_B, r2_B, ...]
    """
    # Pivot slope values
    slope_pivot = slopes_df.pivot(
        index=group_id_label, 
        columns=variable_label, 
        values='slope'
    )
    slope_pivot.columns = [f'slope_{col}' for col in slope_pivot.columns]
    
    # Pivot R² values
    r2_pivot = slopes_df.pivot(
        index=group_id_label, 
        columns=variable_label, 
        values='r2'
    )
    r2_pivot.columns = [f'r2_{col}' for col in r2_pivot.columns]
    
    # Combine
    result = pd.concat([slope_pivot, r2_pivot], axis=1).reset_index()
    
    return result


# =============================================================================
# X_PARAMS EXTRACTION
# =============================================================================

def extract_unique_params(
    df_input: pd.DataFrame,
    group_id_label: str = trans_group_id,
    process_label: str = trans_process_label,
    occurrence_label: str = trans_occurrence_label,
    step_label: str = trans_step_label,
    variable_label: str = trans_variable_label,
    value_label: str = trans_value_norm_label,
    aggregation: str = 'mean'
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract unique process parameters by pivoting input data to wide format.
    
    Each unique (process, occurrence, step, variable) combination becomes a column.
    Column names follow the pattern: {process}_{occurrence}_{step}_{variable}
    
    Parameters
    ----------
    df_input : pd.DataFrame
        Processed input dataframe with process parameters
    group_id_label : str
        Column name for sample/group identifier
    process_label : str
        Column name for process identifier
    occurrence_label : str
        Column name for occurrence number
    step_label : str
        Column name for step number
    variable_label : str
        Column name for parameter/variable identifier
    value_label : str
        Column name for normalized values
    aggregation : str
        How to aggregate if multiple values exist per (sample, param) combination.
        Options: 'mean', 'first', 'last'
    
    Returns
    -------
    X_params : pd.DataFrame
        Wide format DataFrame with group_id as index, parameters as columns
    param_info : pd.DataFrame
        Metadata for each parameter column (process, occurrence, step, variable)
    
    Notes
    -----
    - NaN values are preserved (not filled)
    - Parameters with all NaN values across samples are kept for completeness
    """
    logger = logging.getLogger(__name__)
    
    # Create unique parameter identifier
    df = df_input.copy()
    df['param_name'] = (
        df[process_label].astype(str) + '_' +
        df[occurrence_label].astype(str) + '_' +
        df[step_label].astype(str) + '_' +
        df[variable_label].astype(str)
    )
    
    # Get unique parameters
    unique_params = df['param_name'].unique()
    logger.info(f"Found {len(unique_params)} unique (process, occurrence, step, variable) combinations")
    
    # Aggregate values per (sample, param)
    if aggregation == 'mean':
        agg_func = 'mean'
    elif aggregation == 'first':
        agg_func = 'first'
    elif aggregation == 'last':
        agg_func = 'last'
    else:
        raise ValueError(f"Unknown aggregation method: {aggregation}")
    
    # Build aggregation dict - include class if available
    agg_dict = {
        value_label: agg_func,
        process_label: 'first',
        occurrence_label: 'first',
        step_label: 'first',
        variable_label: 'first'
    }
    if trans_class_label in df.columns:
        agg_dict[trans_class_label] = 'first'
    
    df_agg = df.groupby([group_id_label, 'param_name']).agg(agg_dict).reset_index()
    
    # Pivot to wide format
    X_params = df_agg.pivot(
        index=group_id_label,
        columns='param_name',
        values=value_label
    )
    
    logger.info(f"X_params shape: {X_params.shape} (samples x parameters)")
    
    # Create parameter info DataFrame
    param_info_list = []
    for param in X_params.columns:
        param_row = df_agg[df_agg['param_name'] == param].iloc[0]
        info_dict = {
            'param_name': param,
            'process': param_row[process_label],
            'occurrence': param_row[occurrence_label],
            'step': param_row[step_label],
            'variable': param_row[variable_label]
        }
        # Include class if available (for descriptive purposes, not uniqueness)
        if trans_class_label in df_agg.columns:
            info_dict['class'] = param_row[trans_class_label]
        param_info_list.append(info_dict)
    
    param_info = pd.DataFrame(param_info_list)
    
    return X_params, param_info


# =============================================================================
# HSIC COMPUTATION
# =============================================================================

def rbf_kernel(X: np.ndarray, sigma: float = None) -> np.ndarray:
    """
    Compute RBF (Gaussian) kernel matrix.
    
    K(x, y) = exp(-||x - y||² / (2 * sigma²))
    
    Parameters
    ----------
    X : np.ndarray
        1D array of values (n_samples,)
    sigma : float, optional
        Kernel bandwidth. If None, uses median heuristic.
    
    Returns
    -------
    np.ndarray
        Kernel matrix of shape (n_samples, n_samples)
    """
    X = X.reshape(-1, 1)
    
    # Compute pairwise squared distances
    sq_dists = np.sum(X**2, axis=1, keepdims=True) + \
               np.sum(X**2, axis=1, keepdims=True).T - \
               2 * np.dot(X, X.T)
    
    # Median heuristic for sigma
    if sigma is None:
        # Use median of pairwise distances
        dists = np.sqrt(np.maximum(sq_dists, 0))
        sigma = np.median(dists[dists > 0])
        if sigma == 0:
            sigma = 1.0
    
    # Compute kernel
    K = np.exp(-sq_dists / (2 * sigma**2))
    
    return K


def center_kernel(K: np.ndarray) -> np.ndarray:
    """
    Center a kernel matrix in feature space.
    
    K_centered = H @ K @ H, where H = I - (1/n) * 1 * 1^T
    
    Parameters
    ----------
    K : np.ndarray
        Kernel matrix of shape (n, n)
    
    Returns
    -------
    np.ndarray
        Centered kernel matrix
    """
    n = K.shape[0]
    one_n = np.ones((n, n)) / n
    K_centered = K - one_n @ K - K @ one_n + one_n @ K @ one_n
    return K_centered


def compute_hsic(
    X: np.ndarray,
    Y: np.ndarray,
    sigma_x: float = None,
    sigma_y: float = None,
    normalize: bool = True
) -> float:
    """
    Compute Hilbert-Schmidt Independence Criterion between X and Y.
    
    HSIC measures statistical dependence (not just linear correlation)
    between two variables using kernel methods.
    
    HSIC(X, Y) = (1/(n-1)²) * trace(K_X @ H @ K_Y @ H)
    
    where H is the centering matrix and K_X, K_Y are RBF kernel matrices.
    
    Parameters
    ----------
    X : np.ndarray
        First variable, 1D array of shape (n_samples,)
    Y : np.ndarray
        Second variable, 1D array of shape (n_samples,)
    sigma_x : float, optional
        Kernel bandwidth for X. If None, uses median heuristic.
    sigma_y : float, optional
        Kernel bandwidth for Y. If None, uses median heuristic.
    normalize : bool
        If True, returns normalized HSIC (between 0 and 1).
        Uses HSIC(X,Y) / sqrt(HSIC(X,X) * HSIC(Y,Y))
    
    Returns
    -------
    float
        HSIC value. Higher values indicate stronger dependence.
    
    Notes
    -----
    - Handles NaN values by excluding those samples from computation
    - Returns NaN if fewer than 2 valid samples remain
    """
    # Remove NaN values (from both X and Y where either has NaN)
    mask = ~(np.isnan(X) | np.isnan(Y))
    X_clean = X[mask]
    Y_clean = Y[mask]
    
    n = len(X_clean)
    
    if n < 2:
        return np.nan
    
    # Compute kernel matrices
    K_x = rbf_kernel(X_clean, sigma_x)
    K_y = rbf_kernel(Y_clean, sigma_y)
    
    # Center kernels
    K_x_c = center_kernel(K_x)
    K_y_c = center_kernel(K_y)
    
    # Compute HSIC
    hsic = np.trace(K_x_c @ K_y_c) / ((n - 1) ** 2)
    
    if normalize:
        # Compute HSIC(X, X) and HSIC(Y, Y) for normalization
        hsic_xx = np.trace(K_x_c @ K_x_c) / ((n - 1) ** 2)
        hsic_yy = np.trace(K_y_c @ K_y_c) / ((n - 1) ** 2)
        
        denominator = np.sqrt(hsic_xx * hsic_yy)
        if denominator > 0:
            hsic = hsic / denominator
        else:
            hsic = 0.0
    
    return hsic


def compute_baseline_hsic(
    Y_slope: np.ndarray,
    n_samples: int,
    seed: int = 42
) -> pd.DataFrame:
    """
    Compute HSIC for baseline signals (random noise and constant).
    
    This provides reference values to interpret real HSIC scores:
    - Random signals should yield HSIC ≈ 0 (no statistical dependence)
    - Constant signal yields HSIC = 0 or NaN (no variance)
    
    Parameters
    ----------
    Y_slope : np.ndarray
        Target slope values (n_samples,)
    n_samples : int
        Number of samples (should match len(Y_slope))
    seed : int
        Random seed for reproducibility
    
    Returns
    -------
    pd.DataFrame
        Results with columns: baseline_name, hsic_score, description, valid_samples
    """
    logger = logging.getLogger(__name__)
    logger.info("Computing baseline HSIC scores...")
    
    rng = np.random.default_rng(seed)
    
    # Define baseline signals
    baselines = {
        'baseline_random_uniform': {
            'signal': rng.uniform(0, 1, n_samples),
            'description': 'Uniform random in [0, 1]'
        },
        'baseline_random_normal': {
            'signal': rng.standard_normal(n_samples),
            'description': 'Standard normal N(0, 1)'
        },
        'baseline_constant': {
            'signal': np.ones(n_samples),
            'description': 'Constant value = 1.0 (no variance)'
        }
    }
    
    results = []
    
    for name, baseline in baselines.items():
        X_baseline = baseline['signal']
        
        # Compute HSIC
        hsic_score = compute_hsic(X_baseline, Y_slope, normalize=True)
        
        # Count valid samples
        valid_mask = ~(np.isnan(X_baseline) | np.isnan(Y_slope))
        valid_samples = np.sum(valid_mask)
        
        results.append({
            'baseline_name': name,
            'hsic_score': hsic_score,
            'description': baseline['description'],
            'valid_samples': valid_samples
        })
        
        logger.info(f"  {name}: HSIC = {hsic_score:.6f}")
    
    return pd.DataFrame(results)


def compute_hsic_for_all_params(
    X_params: pd.DataFrame,
    Y_slope: np.ndarray,
    param_info: pd.DataFrame,
    group_ids: np.ndarray = None
) -> pd.DataFrame:
    """
    Compute HSIC between each input parameter and the target slope.
    
    Parameters
    ----------
    X_params : pd.DataFrame
        Wide format parameter matrix (samples x parameters)
    Y_slope : np.ndarray
        Target slope values (n_samples,)
    param_info : pd.DataFrame
        Parameter metadata with columns: param_name, process, occurrence, step, variable
    group_ids : np.ndarray, optional
        Sample IDs to use. If None, uses all samples in X_params.
    
    Returns
    -------
    pd.DataFrame
        Results with columns: param_name, process, occurrence, step, variable, 
        hsic_score, valid_samples, rank
    """
    logger = logging.getLogger(__name__)
    
    if group_ids is not None:
        X_params = X_params.loc[group_ids]
    
    # Align Y_slope with X_params index
    if isinstance(Y_slope, pd.Series):
        Y_slope = Y_slope.loc[X_params.index].values
    
    results = []
    n_params = len(X_params.columns)
    
    logger.info(f"Computing HSIC for {n_params} parameters...")
    
    for i, param_name in enumerate(X_params.columns):
        if (i + 1) % 50 == 0:
            logger.info(f"  Progress: {i+1}/{n_params}")
        
        X_param = X_params[param_name].values
        
        # Compute HSIC
        hsic_score = compute_hsic(X_param, Y_slope, normalize=True)
        
        # Count valid samples
        valid_mask = ~(np.isnan(X_param) | np.isnan(Y_slope))
        valid_samples = np.sum(valid_mask)
        
        # Get parameter info
        info_row = param_info[param_info['param_name'] == param_name].iloc[0]
        
        result_dict = {
            'param_name': param_name,
            'process': info_row['process'],
            'occurrence': info_row['occurrence'],
            'step': info_row['step'],
            'variable': info_row['variable'],
            'hsic_score': hsic_score,
            'valid_samples': valid_samples
        }
        # Include class if available (for descriptive purposes)
        if 'class' in info_row.index:
            result_dict['class'] = info_row['class']
        
        results.append(result_dict)
    
    result_df = pd.DataFrame(results)
    
    # Add ranking (higher HSIC = better = rank 1)
    result_df['rank'] = result_df['hsic_score'].rank(ascending=False, method='min')
    result_df = result_df.sort_values('rank')
    
    logger.info(f"HSIC computation complete. Top 5 parameters:")
    for _, row in result_df.head(5).iterrows():
        logger.info(f"  {row['param_name']}: HSIC={row['hsic_score']:.4f}")
    
    return result_df


# =============================================================================
# MAIN ANALYSIS FUNCTION
# =============================================================================

def run_hsic_analysis(
    dataset_id: str,
    output_dir: str = None,
    aggregation: str = 'mean'
) -> Dict[str, pd.DataFrame]:
    """
    Run complete HSIC analysis pipeline.
    
    This function:
    1. Loads processed df_input.parquet and df_trg.csv
    2. Computes slopes (Sense_A, Sense_B separately) via linear regression
    3. Extracts unique X_params from input data
    4. Computes HSIC for each X_param vs Y_slope (separately for each sense)
    5. Saves results as CSV files
    
    Caching Behavior
    ----------------
    The function checks if output files already exist in the output directory.
    If they exist, they are loaded from disk instead of being recomputed.
    To force recomputation, simply delete the corresponding output files.
    
    Parameters
    ----------
    dataset_id : str
        Dataset identifier (folder name in data/builds/)
    output_dir : str, optional
        Output directory for results. If None, saves to 
        data/builds/{dataset_id}/output/hsic_analysis/
    aggregation : str
        Aggregation method for input parameters: 'mean', 'first', 'last'
    
    Returns
    -------
    dict
        Dictionary with keys:
        - 'slopes': DataFrame with slope and R² values per sample
        - 'hsic_A': HSIC results for Sense_A
        - 'hsic_B': HSIC results for Sense_B
        - 'param_info': Parameter metadata
    
    Output Files
    ------------
    - slopes_summary.csv: Slopes and R² for all samples
    - hsic_results_A.csv: HSIC ranking for Sense_A
    - hsic_results_B.csv: HSIC ranking for Sense_B
    - hsic_baseline_A.csv: Baseline HSIC scores for Sense_A (random/constant signals)
    - hsic_baseline_B.csv: Baseline HSIC scores for Sense_B (random/constant signals)
    - param_info.csv: Parameter metadata
    
    Examples
    --------
    >>> from data_analysis.hsic import run_hsic_analysis
    >>> results = run_hsic_analysis(dataset_id="my_dataset")
    >>> print(results['hsic_A'].head())  # Top parameters for Sense_A
    """
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    logger.info(f"Starting HSIC analysis for dataset: {dataset_id}")
    
    # =========================================================================
    # SETUP
    # =========================================================================
    ROOT_DIR = get_root_dir()
    INPUT_DIR, OUTPUT_DIR_DEFAULT, CONTROL_DIR = get_dirs(ROOT_DIR, dataset_id)
    
    if output_dir is None:
        output_dir = join(OUTPUT_DIR_DEFAULT, "hsic_analysis")
    
    if not exists(output_dir):
        makedirs(output_dir)
        logger.info(f"Created output directory: {output_dir}")
    
    # Define file paths
    slopes_path = join(output_dir, "slopes_summary.csv")
    param_info_path = join(output_dir, "param_info.csv")
    
    # =========================================================================
    # CHECK FOR CACHED SLOPES AND PARAM_INFO
    # =========================================================================
    if exists(slopes_path) and exists(param_info_path):
        logger.info("Found cached slopes_summary.csv and param_info.csv, loading from disk...")
        slopes_wide = pd.read_csv(slopes_path)
        param_info = pd.read_csv(param_info_path)
        logger.info(f"Loaded slopes_wide: {slopes_wide.shape}")
        logger.info(f"Loaded param_info: {param_info.shape}")
    else:
        # =================================================================
        # LOAD DATA
        # =================================================================
        logger.info("Loading data...")
        
        # Load target data
        df_trg_path = join(CONTROL_DIR, target_filename)
        if not exists(df_trg_path):
            raise FileNotFoundError(f"Target file not found: {df_trg_path}")
        df_trg = safe_read_csv(df_trg_path, sep=target_sep)
        logger.info(f"Loaded df_trg: {df_trg.shape}")
        
        # Load input data
        df_input_path = join(OUTPUT_DIR_DEFAULT, trans_df_input)
        if not exists(df_input_path):
            raise FileNotFoundError(
                f"Processed input file not found: {df_input_path}\n"
                f"Run the main pipeline first to create this file."
            )
        df_input = pd.read_parquet(df_input_path)
        logger.info(f"Loaded df_input: {df_input.shape}")
        
        # =================================================================
        # COMPUTE SLOPES
        # =================================================================
        logger.info("Computing slopes from target time-series...")
        slopes_df = compute_slopes(df_trg)
        slopes_wide = pivot_slopes(slopes_df)
        
        logger.info(f"Slopes computed for {len(slopes_wide)} samples")
        logger.info(f"Slope columns: {[c for c in slopes_wide.columns if 'slope' in c]}")
        
        # Save slopes
        slopes_wide.to_csv(slopes_path, index=False)
        logger.info("Saved slopes_summary.csv")
        
        # =================================================================
        # EXTRACT UNIQUE PARAMETERS
        # =================================================================
        logger.info("Extracting unique input parameters...")
        X_params, param_info = extract_unique_params(df_input, aggregation=aggregation)
        
        # Save parameter info
        param_info.to_csv(param_info_path, index=False)
        logger.info(f"Saved param_info.csv ({len(param_info)} parameters)")
    
    # =========================================================================
    # PREPARE FOR HSIC COMPUTATION (need X_params if not loaded)
    # =========================================================================
    # Check if we need to load input data for HSIC computation
    # We need X_params for HSIC, so we need to extract it if not already done
    
    # Load input data if needed for HSIC computation
    df_input_path = join(OUTPUT_DIR_DEFAULT, trans_df_input)
    if not exists(df_input_path):
        raise FileNotFoundError(
            f"Processed input file not found: {df_input_path}\n"
            f"Run the main pipeline first to create this file."
        )
    df_input = pd.read_parquet(df_input_path)
    X_params, _ = extract_unique_params(df_input, aggregation=aggregation)
    
    # =========================================================================
    # FIND COMMON SAMPLES
    # =========================================================================
    # Get samples that exist in both X_params and slopes
    common_samples = list(set(X_params.index) & set(slopes_wide[trans_group_id]))
    logger.info(f"Found {len(common_samples)} common samples")
    
    if len(common_samples) == 0:
        raise ValueError("No common samples found between input and target!")
    
    # Filter to common samples
    X_params_common = X_params.loc[common_samples]
    slopes_common = slopes_wide[slopes_wide[trans_group_id].isin(common_samples)]
    slopes_common = slopes_common.set_index(trans_group_id).loc[common_samples]
    
    # =========================================================================
    # COMPUTE HSIC FOR EACH SENSE
    # =========================================================================
    results = {
        'slopes': slopes_wide,
        'param_info': param_info
    }
    
    # Get slope columns (e.g., slope_A, slope_B or slope_1, slope_2)
    slope_cols = [c for c in slopes_common.columns if c.startswith('slope_')]
    
    for slope_col in slope_cols:
        sense_id = slope_col.replace('slope_', '')
        
        # Define file paths for this sense
        hsic_results_path = join(output_dir, f"hsic_results_{sense_id}.csv")
        baseline_path = join(output_dir, f"hsic_baseline_{sense_id}.csv")
        
        # Check if HSIC results already exist
        if exists(hsic_results_path):
            logger.info(f"Found cached hsic_results_{sense_id}.csv, loading from disk...")
            hsic_results = pd.read_csv(hsic_results_path)
            results[f'hsic_{sense_id}'] = hsic_results
        else:
            logger.info(f"\n{'='*50}")
            logger.info(f"Computing HSIC for Sense {sense_id}...")
            logger.info(f"{'='*50}")
            
            Y_slope = slopes_common[slope_col].values
            
            hsic_results = compute_hsic_for_all_params(
                X_params_common,
                Y_slope,
                param_info
            )
            
            # Save results
            hsic_results.to_csv(hsic_results_path, index=False)
            logger.info(f"Saved hsic_results_{sense_id}.csv")
            
            results[f'hsic_{sense_id}'] = hsic_results
        
        # Check if baseline results already exist
        if exists(baseline_path):
            logger.info(f"Found cached hsic_baseline_{sense_id}.csv, loading from disk...")
            baseline_results = pd.read_csv(baseline_path)
            results[f'baseline_{sense_id}'] = baseline_results
        else:
            logger.info(f"\nComputing baseline HSIC for Sense {sense_id}...")
            Y_slope = slopes_common[slope_col].values
            
            baseline_results = compute_baseline_hsic(
                Y_slope=Y_slope,
                n_samples=len(Y_slope),
                seed=42
            )
            
            # Save baseline results
            baseline_results.to_csv(baseline_path, index=False)
            logger.info(f"Saved hsic_baseline_{sense_id}.csv")
            
            results[f'baseline_{sense_id}'] = baseline_results
    
    # =========================================================================
    # SUMMARY
    # =========================================================================
    logger.info("\n" + "="*60)
    logger.info("HSIC ANALYSIS COMPLETE")
    logger.info("="*60)
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Files saved:")
    logger.info(f"  - slopes_summary.csv")
    logger.info(f"  - param_info.csv")
    for slope_col in slope_cols:
        sense_id = slope_col.replace('slope_', '')
        logger.info(f"  - hsic_results_{sense_id}.csv")
        logger.info(f"  - hsic_baseline_{sense_id}.csv")
    
    return results


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) > 1:
        dataset_id = sys.argv[1]
    else:
        dataset_id = "dyconex_251117"  # Default dataset
    
    results = run_hsic_analysis(dataset_id=dataset_id)
    
    # Print summary
    print("\n" + "="*60)
    print("TOP 10 PARAMETERS BY HSIC SCORE")
    print("="*60)
    
    for key in results:
        if key.startswith('hsic_'):
            sense = key.replace('hsic_', '')
            print(f"\nSense {sense}:")
            print(results[key][['param_name', 'hsic_score', 'rank']].head(10).to_string(index=False))
