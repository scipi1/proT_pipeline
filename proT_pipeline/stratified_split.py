"""
Stratified splitting utilities for proT_pipeline dataset generation.

This module provides functions to perform stratified train-test splits based on
sample-level metrics (e.g., rarity, complexity, etc.). The splits ensure balanced
representation across the metric distribution.

The workflow:
1. Metrics are computed and saved as parquet by generate_dataset.py
2. Split functions read these metrics and create stratified splits
3. Splits are applied to numpy arrays (X, Y)
"""

import numpy as np
import pandas as pd
import logging
from typing import Tuple, Optional, Literal
from os.path import exists, join


def stratified_split_by_metric(
    X: np.ndarray,
    Y: np.ndarray,
    metrics_df: pd.DataFrame,
    metric_column: str,
    group_id_column: str,
    train_ratio: float = 0.8,
    n_bins: int = 50,
    leave_residuals: Optional[Literal["train", "test"]] = "train",
    shuffle: bool = False,
    seed: Optional[int] = None,
    S: Optional[np.ndarray] = None
) -> Tuple[np.ndarray, ...]:
    """
    Perform stratified train-test split based on a metric column.
    
    This function ensures that each metric bin maintains the same train-test ratio,
    providing balanced representation across the metric distribution (e.g., rare
    and common samples).
    
    Parameters
    ----------
    X : np.ndarray
        Input array of shape (N, L, D_x) where N is number of samples
    Y : np.ndarray
        Target array of shape (N, L, D_y) where N is number of samples
    metrics_df : pd.DataFrame
        DataFrame containing sample metrics with group_id and metric columns
    metric_column : str
        Name of the column in metrics_df to use for stratification
        (e.g., 'rarity_last_value', 'rarity_nan_fraction')
    group_id_column : str
        Name of the column in metrics_df containing sample identifiers
    train_ratio : float, optional
        Fraction of data for training (default: 0.8)
    n_bins : int, optional
        Number of bins for metric stratification (default: 50)
    leave_residuals : Optional[Literal["train", "test"]], optional
        Where to place samples from bins too small to split.
        - "train": put all residuals in training set
        - "test": put all residuals in test set
        - None: distribute residuals randomly
        (default: "train")
    shuffle : bool, optional
        Whether to shuffle samples within each bin before splitting.
        If False, takes first train_ratio samples for training.
        (default: False)
    seed : Optional[int], optional
        Random seed for reproducibility when shuffle=True (default: None)
    S : np.ndarray, optional
        Source array of shape (N, L, D_s) where N is number of samples.
        Used when split_input_by_class=True (CausaliT format with S, X, Y).
        If provided, returns (S_train, S_test, X_train, X_test, Y_train, Y_test).
        (default: None)
    
    Returns
    -------
    If S is None:
        X_train, X_test, Y_train, Y_test : np.ndarray
    If S is provided:
        S_train, S_test, X_train, X_test, Y_train, Y_test : np.ndarray
    
    Raises
    ------
    ValueError
        If metric_column or group_id_column not found in metrics_df
        If sample IDs in metrics_df don't match array indices
    
    Examples
    --------
    >>> # After computing metrics and saving to parquet
    >>> metrics_df = pd.read_parquet('sample_metrics.parquet')
    >>> X_train, X_test, Y_train, Y_test = stratified_split_by_metric(
    ...     X, Y, metrics_df,
    ...     metric_column='rarity_last_value',
    ...     group_id_column='group',
    ...     train_ratio=0.8,
    ...     n_bins=50,
    ...     seed=42
    ... )
    >>> # With S array (CausaliT format)
    >>> S_train, S_test, X_train, X_test, Y_train, Y_test = stratified_split_by_metric(
    ...     X, Y, metrics_df,
    ...     metric_column='rarity_last_value',
    ...     group_id_column='group',
    ...     S=S,
    ...     seed=42
    ... )
    
    Notes
    -----
    - Bins are created based on metric quantiles for better balance
    - Minimum split requires at least 2 samples per bin
    - With train_ratio=0.8, bins with <5 samples cannot be split evenly
    - Sample IDs in metrics_df should correspond to array indices
    """
    logger = logging.getLogger(__name__)
    
    # Validate inputs
    if metric_column not in metrics_df.columns:
        raise ValueError(f"Metric column '{metric_column}' not found in metrics_df. "
                        f"Available columns: {list(metrics_df.columns)}")
    
    if group_id_column not in metrics_df.columns:
        raise ValueError(f"Group ID column '{group_id_column}' not found in metrics_df. "
                        f"Available columns: {list(metrics_df.columns)}")
    
    if seed is not None and shuffle:
        np.random.seed(seed)
    
    # Get number of samples
    n_samples = X.shape[0]
    
    # Extract sample IDs from X and Y arrays (stored in first feature at position 0)
    X_ids = X[:, 0, 0].astype(int)
    Y_ids = Y[:, 0, 0].astype(int)
    
    # Validate that X and Y have the same sample IDs in the same order
    if not np.array_equal(X_ids, Y_ids):
        raise ValueError(
            "X and Y arrays have different sample IDs or different ordering. "
            "X and Y must correspond to the same samples in the same order."
        )
    
    logger.info(f"Validated: X and Y have matching sample IDs (n={n_samples})")
    
    # Filter metrics_df to only include samples present in X and Y
    available_ids = set(X_ids)
    metrics_in_arrays = metrics_df[group_id_column].isin(available_ids)
    n_metrics_before = len(metrics_df)
    
    # Create a copy and filter to available samples
    metrics_copy = metrics_df[metrics_in_arrays].copy()
    n_metrics_after = len(metrics_copy)
    
    if n_metrics_after == 0:
        raise ValueError(
            f"No samples found in metrics_df that match the IDs in X and Y arrays. "
            f"metrics_df has IDs: {metrics_df[group_id_column].unique()[:10]}... "
            f"X/Y have IDs: {X_ids[:10]}..."
        )
    
    if n_metrics_before != n_metrics_after:
        logger.warning(
            f"Filtered metrics_df from {n_metrics_before} to {n_metrics_after} samples "
            f"to match available IDs in X and Y arrays."
        )
    
    # Create mapping from group_id to array index
    id_to_index = {group_id: idx for idx, group_id in enumerate(X_ids)}
    
    logger.info(f"Created ID-to-index mapping for {len(id_to_index)} samples")
    
    # Assign metric bins using quantiles for better balance
    try:
        metrics_copy['metric_bin'] = pd.qcut(
            metrics_copy[metric_column], 
            q=n_bins, 
            labels=False, 
            duplicates='drop'  # Handle case where we have fewer unique values than bins
        )
    except ValueError as e:
        logger.warning(f"Could not create {n_bins} bins. Reducing bin count. Error: {e}")
        # Fallback: use fewer bins
        unique_values = metrics_copy[metric_column].nunique()
        n_bins_actual = min(n_bins, unique_values)
        metrics_copy['metric_bin'] = pd.qcut(
            metrics_copy[metric_column], 
            q=n_bins_actual, 
            labels=False, 
            duplicates='drop'
        )
        logger.info(f"Using {n_bins_actual} bins instead of {n_bins}")
    
    train_group_ids = []
    test_group_ids = []
    
    # Process each bin separately
    for bin_id in metrics_copy['metric_bin'].unique():
        bin_mask = metrics_copy['metric_bin'] == bin_id
        bin_group_ids = metrics_copy[bin_mask][group_id_column].values
        n_samples_bin = len(bin_group_ids)
        
        # Calculate train size
        n_train = int(n_samples_bin * train_ratio)
        n_test = n_samples_bin - n_train
        
        # Check if we can perform the split
        if n_train == 0 or n_test == 0:
            # Bin too small to split - handle residuals
            if leave_residuals == "train":
                train_group_ids.extend(bin_group_ids)
            elif leave_residuals == "test":
                test_group_ids.extend(bin_group_ids)
            else:  # leave_residuals is None - random assignment
                if np.random.rand() < train_ratio:
                    train_group_ids.extend(bin_group_ids)
                else:
                    test_group_ids.extend(bin_group_ids)
        else:
            # Shuffle if requested, otherwise use original order
            if shuffle:
                bin_group_ids = np.random.permutation(bin_group_ids)
            
            train_group_ids.extend(bin_group_ids[:n_train])
            test_group_ids.extend(bin_group_ids[n_train:])
    
    # Convert group IDs to array indices using the mapping
    train_indices = np.array([id_to_index[gid] for gid in train_group_ids], dtype=int)
    test_indices = np.array([id_to_index[gid] for gid in test_group_ids], dtype=int)
    
    # Split the arrays
    X_train = X[train_indices]
    X_test = X[test_indices]
    Y_train = Y[train_indices]
    Y_test = Y[test_indices]
    
    # Split S array if provided
    if S is not None:
        S_train = S[train_indices]
        S_test = S[test_indices]
    
    # Print split statistics
    logger.info(f"Stratified Split Statistics (metric: {metric_column}):")
    logger.info(f"  Total samples: {n_samples}")
    logger.info(f"  Train samples: {len(train_indices)} ({len(train_indices)/n_samples*100:.1f}%)")
    logger.info(f"  Test samples: {len(test_indices)} ({len(test_indices)/n_samples*100:.1f}%)")
    logger.info(f"  Number of metric bins used: {metrics_copy['metric_bin'].nunique()}")
    logger.info(f"  Shuffled within bins: {shuffle}")
    if S is not None:
        logger.info(f"  S array included: Yes")
    
    # Check balance across metric bins
    train_metric_values = metrics_copy[metrics_copy[group_id_column].isin(train_group_ids)][metric_column]
    test_metric_values = metrics_copy[metrics_copy[group_id_column].isin(test_group_ids)][metric_column]
    
    logger.info(f"  Metric distribution preserved:")
    logger.info(f"    Train {metric_column} range: [{train_metric_values.min():.3f}, {train_metric_values.max():.3f}]")
    logger.info(f"    Test {metric_column} range: [{test_metric_values.min():.3f}, {test_metric_values.max():.3f}]")
    
    # Return with S if provided
    if S is not None:
        return S_train, S_test, X_train, X_test, Y_train, Y_test
    else:
        return X_train, X_test, Y_train, Y_test


def stratified_split_from_file(
    X: np.ndarray,
    Y: np.ndarray,
    metrics_file_path: str,
    metric_column: str,
    group_id_column: str = 'group',
    train_ratio: float = 0.8,
    n_bins: int = 50,
    leave_residuals: Optional[Literal["train", "test"]] = "train",
    shuffle: bool = False,
    seed: Optional[int] = None,
    S: Optional[np.ndarray] = None
) -> Tuple[np.ndarray, ...]:
    """
    Perform stratified split by reading metrics from a saved parquet file.
    
    This is a convenience wrapper around stratified_split_by_metric that handles
    loading the metrics DataFrame from disk.
    
    Parameters
    ----------
    X : np.ndarray
        Input array of shape (N, L, D_x)
    Y : np.ndarray
        Target array of shape (N, L, D_y)
    metrics_file_path : str
        Path to the parquet file containing sample metrics
    metric_column : str
        Name of the column to use for stratification
    group_id_column : str, optional
        Name of the group ID column (default: 'group')
    train_ratio : float, optional
        Fraction of data for training (default: 0.8)
    n_bins : int, optional
        Number of bins for stratification (default: 50)
    leave_residuals : Optional[Literal["train", "test"]], optional
        Where to place residual samples (default: "train")
    shuffle : bool, optional
        Whether to shuffle within bins (default: False)
    seed : Optional[int], optional
        Random seed (default: None)
    S : np.ndarray, optional
        Source array of shape (N, L, D_s). Used when split_input_by_class=True.
        If provided, returns (S_train, S_test, X_train, X_test, Y_train, Y_test).
        (default: None)
    
    Returns
    -------
    If S is None:
        X_train, X_test, Y_train, Y_test : np.ndarray
    If S is provided:
        S_train, S_test, X_train, X_test, Y_train, Y_test : np.ndarray
    
    Raises
    ------
    FileNotFoundError
        If metrics file doesn't exist
    
    Examples
    --------
    >>> X_train, X_test, Y_train, Y_test = stratified_split_from_file(
    ...     X, Y,
    ...     metrics_file_path='data/builds/my_dataset/output/sample_metrics.parquet',
    ...     metric_column='rarity_last_value',
    ...     train_ratio=0.8,
    ...     n_bins=50,
    ...     seed=42
    ... )
    >>> # With S array (CausaliT format)
    >>> S_train, S_test, X_train, X_test, Y_train, Y_test = stratified_split_from_file(
    ...     X, Y,
    ...     metrics_file_path='sample_metrics.parquet',
    ...     metric_column='rarity_last_value',
    ...     S=S,
    ...     seed=42
    ... )
    """
    logger = logging.getLogger(__name__)
    
    # Check if file exists
    if not exists(metrics_file_path):
        raise FileNotFoundError(
            f"Metrics file not found: {metrics_file_path}\n"
            f"Make sure to run generate_dataset with compute_metrics=True first."
        )
    
    # Load metrics
    logger.info(f"Loading metrics from: {metrics_file_path}")
    metrics_df = pd.read_parquet(metrics_file_path)
    logger.info(f"Loaded metrics with shape: {metrics_df.shape}")
    logger.info(f"Available metrics: {[col for col in metrics_df.columns if col != group_id_column]}")
    
    # Perform split
    return stratified_split_by_metric(
        X, Y, metrics_df,
        metric_column=metric_column,
        group_id_column=group_id_column,
        train_ratio=train_ratio,
        n_bins=n_bins,
        leave_residuals=leave_residuals,
        shuffle=shuffle,
        seed=seed,
        S=S
    )
