import logging
from os.path import dirname, join, abspath, exists
from os import makedirs
import sys
import numpy as np
from omegaconf import OmegaConf
ROOT_DIR = dirname(dirname(abspath(__file__)))
sys.path.append(ROOT_DIR)
from proT_pipeline.labels import *
from proT_pipeline.input_processing.assemble_raw import assemble_raw
from proT_pipeline.input_processing.process_raw import process_raw
from proT_pipeline.input_processing.generate_dataset import generate_dataset
from proT_pipeline.input_processing.get_idx_from_id import get_idx_from_id
from proT_pipeline.input_processing.split_by_metric import split_by_metric



def main(
    dataset_id: str, 
    missing_threshold: float, 
    select_test: bool = False,
    use_stratified_split: bool = False,
    stratified_metric: str = 'rarity_last_value',
    train_ratio: float = 0.8,
    n_bins: int = 50,
    split_shuffle: bool = False,
    split_seed: int = 42,
    grouping_method: str = 'panel', 
    grouping_column: str = None,
    selected_processes: list = None,
    split_input_by_class: bool = False,
    debug: bool = False):
    """
    Dyconex dataset assembly according to the control files
    Folder structure:
    data
        |__input              | process files here
        |__builds             |
            |__dataset_id     | must be created beforehand!
                |__control    | control files here
                |__output     | output files here
    
    Args:
        dataset_id (str): name of dataset folder, must be created beforehand!
        missing_threshold (float): threshold for missing data filtering
        select_test (bool): if True, uses explicit test ID selection (legacy method)
        use_stratified_split (bool): if True, performs stratified split based on metrics
        stratified_metric (str): metric column to use for stratification (e.g., 'rarity_last_value')
        train_ratio (float): train/test split ratio (default: 0.8)
        n_bins (int): number of bins for stratification (default: 50)
        split_shuffle (bool): whether to shuffle within bins (default: False)
        split_seed (int): random seed for reproducibility (default: 42)
        grouping_method (str): grouping method ('panel' or 'column')
        grouping_column (str): if method is 'column', specify which column
        selected_processes (list, optional): list of process labels to load 
            (e.g., ["Laser", "Plasma"]). If None, all processes are loaded.
        split_input_by_class (bool): if True, split input data into S (Set params, class=2)
            and X (Read params, class=1), producing three tensors (S, X, Y) compatible
            with CausaliT format. Default False maintains backward compatibility.
        debug (bool, optional): if True assembles a slice of target. Defaults to False.
    
    Notes:
        - select_test and use_stratified_split are mutually exclusive
        - For publication-quality datasets, use use_stratified_split=True
        - Stratified split requires pre-computed metrics (sample_metrics.parquet)
    """
    
    
    ROOT_DIR = dirname(dirname(abspath(__file__)))
    sys.path.append(ROOT_DIR)
    _,OUTPUT_DIR,_ = get_dirs(ROOT_DIR, dataset_id)
    
    if not(exists(OUTPUT_DIR)):
        makedirs(OUTPUT_DIR)
    
    
    
    
    log_filename = join(OUTPUT_DIR, "process_chain_build.log")
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",)
    
   
    
    logging.info("Assembling raw process dataframe...")
    assemble_raw(
        dataset_id=dataset_id, 
        grouping_method=grouping_method, 
        grouping_column=grouping_column,
        selected_processes=selected_processes,
        debug=debug)
    logging.info("Raw dataframe assembly complete")

    logging.info("Processing raw dataframe...")
    process_raw(dataset_id=dataset_id, missing_threshold=missing_threshold )
    logging.info("Raw dataframe processing complete")

    logging.info("Generating dataset...")
    generate_dataset(dataset_id=dataset_id, split_input_by_class=split_input_by_class)
    logging.info("Dataset generation complete")
    
    
    if select_test:
        logging.info("Exporting selected indices...")
        get_idx_from_id(dataset_id=dataset_id,
                        id_sel_filename="selected_id.npy", 
                        idx_sel_filename="test_ds_idx.npy")
        logging.info("Index selection complete")
        
    if use_stratified_split:
        logging.info("Performing stratified split...")
        split_by_metric(dataset_id=dataset_id,
                       metric_column=stratified_metric,
                       train_ratio=train_ratio,
                       n_bins=n_bins,
                       shuffle=split_shuffle,
                       seed=split_seed)
        logging.info("Stratified split complete")



if __name__ == "__main__":
    main(
        dataset_id = "dyconex_test_params_class",
        missing_threshold=30,
        use_stratified_split = True,
        debug=False)
