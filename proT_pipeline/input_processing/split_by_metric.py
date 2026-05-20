"""
Wrapper for stratified split that follows the proT_pipeline pattern.

Similar to get_idx_from_id.py, this module provides a simple function
to perform stratified splits on already generated datasets.
"""

import numpy as np
from os.path import join
from proT_pipeline.labels import *
from proT_pipeline.stratified_split import stratified_split_from_file


def split_by_metric(
    dataset_id: str,
    metric_column: str = 'rarity_last_value',
    train_ratio: float = 0.8,
    n_bins: int = 50,
    shuffle: bool = False,
    seed: int = 42
):
    """
    Perform stratified train-test split based on pre-computed metrics.
    
    This function loads the dataset arrays and metrics, performs stratified
    splitting, and saves the train/test splits.
    
    Parameters
    ----------
    dataset_id : str
        Dataset folder name in builds directory
    metric_column : str, optional
        Metric column to use for stratification (default: 'rarity_last_value')
    train_ratio : float, optional
        Train/test split ratio (default: 0.8)
    n_bins : int, optional
        Number of bins for stratification (default: 50)
    shuffle : bool, optional
        Whether to shuffle within bins (default: False)
    seed : int, optional
        Random seed for reproducibility (default: 42)
    
    Notes
    -----
    Requires sample_metrics.parquet to exist in the output directory.
    Saves: train_data.npz, test_data.npz
    
    When S array exists (split_input_by_class=True was used):
    - train_data.npz contains s, x, y arrays
    - test_data.npz contains s, x, y arrays
    
    Otherwise (backward compatible):
    - train_data.npz contains x, y arrays
    - test_data.npz contains x, y arrays
    """
    
    # Define directories
    ROOT_DIR = get_root_dir()
    _, OUTPUT_DIR, _ = get_dirs(ROOT_DIR, dataset_id)
    
    dataset_name = f"ds_{dataset_id}"
    dataset_dir = join(OUTPUT_DIR, dataset_name)
    
    # Load dataset arrays from compressed npz file
    data = np.load(join(dataset_dir, full_data_label))
    X = data['x']
    Y = data['y']
    
    # Check if S array exists (split_input_by_class=True was used)
    has_S = 's' in data.files
    S = data['s'] if has_S else None
    
    # Metrics file path
    metrics_file = join(OUTPUT_DIR, "sample_metrics.parquet")
    
    # Perform stratified split
    if has_S:
        S_train, S_test, X_train, X_test, Y_train, Y_test = stratified_split_from_file(
            X, Y,
            metrics_file_path=metrics_file,
            metric_column=metric_column,
            group_id_column=trans_group_id,
            train_ratio=train_ratio,
            n_bins=n_bins,
            shuffle=shuffle,
            seed=seed,
            S=S
        )
        
        # Save split datasets with S array
        np.savez(join(dataset_dir, train_data_label), s=S_train, x=X_train, y=Y_train)
        np.savez(join(dataset_dir, test_data_label), s=S_test, x=X_test, y=Y_test)
        print(f"Saved train dataset to {train_data_label} (S: {S_train.shape}, X: {X_train.shape}, Y: {Y_train.shape})")
        print(f"Saved test dataset to {test_data_label} (S: {S_test.shape}, X: {X_test.shape}, Y: {Y_test.shape})")
    else:
        X_train, X_test, Y_train, Y_test = stratified_split_from_file(
            X, Y,
            metrics_file_path=metrics_file,
            metric_column=metric_column,
            group_id_column=trans_group_id,
            train_ratio=train_ratio,
            n_bins=n_bins,
            shuffle=shuffle,
            seed=seed
        )
        
        # Save split datasets as compressed numpy archives (npz format)
        np.savez(join(dataset_dir, train_data_label), x=X_train, y=Y_train)
        np.savez(join(dataset_dir, test_data_label), x=X_test, y=Y_test)
        print(f"Saved train dataset to {train_data_label} (X: {X_train.shape}, Y: {Y_train.shape})")
        print(f"Saved test dataset to {test_data_label} (X: {X_test.shape}, Y: {Y_test.shape})")


if __name__ == "__main__":
    split_by_metric(
        dataset_id="dx_250806_panel_200_pad",
        metric_column='rarity_last_value',
        train_ratio=0.8,
        n_bins=50,
        seed=42
    )
