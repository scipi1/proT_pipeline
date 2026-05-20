import logging
from os.path import dirname, join, abspath, exists
from os import makedirs
import sys
from typing import Optional
from omegaconf import OmegaConf

ROOT_DIR = dirname(dirname(abspath(__file__)))
sys.path.append(ROOT_DIR)

from proT_pipeline.labels import *
from proT_pipeline.input_processing.assemble_raw import assemble_raw
from proT_pipeline.input_processing.process_raw import process_raw
from proT_pipeline.input_processing.filter_sparse_columns import filter_sparse_columns
from proT_pipeline.input_processing.filter_by_hsic import filter_by_hsic
from proT_pipeline.input_processing.generate_dataset import generate_dataset
from proT_pipeline.input_processing.generate_tabular_dataset import generate_tabular_dataset
from proT_pipeline.target_processing.compute_ist_slopes import compute_ist_slopes
from proT_pipeline.input_processing.get_idx_from_id import get_idx_from_id
from proT_pipeline.input_processing.split_by_metric import split_by_metric


def main(
    dataset_id: str,
    # ------------------------------------------------------------------
    # Pipeline mode
    # ------------------------------------------------------------------
    mode: str = "sequential",          # "sequential" or "tabular"
    # ------------------------------------------------------------------
    # Shared column-level sparsity filter (applied before HSIC/generation)
    # Replaces: missing_threshold (sequential) + tabular_col_threshold (tabular)
    # ------------------------------------------------------------------
    col_missing_threshold: float = 50.0,
    col_min_nonmissing_count: int = 5,
    col_iterative_cleaning: bool = False,
    col_filter_dry_run: bool = False,
    # ------------------------------------------------------------------
    # Assembly
    # ------------------------------------------------------------------
    grouping_method: str = "panel",
    grouping_column: Optional[str] = None,
    selected_processes: Optional[list] = None,
    debug: bool = False,
    # ------------------------------------------------------------------
    # IST slope backbone (always runs)
    # ------------------------------------------------------------------
    ist_fail_threshold: float = 1.0 / 200.0,
    ist_norm_method: str = "minmax",
    ist_force_recompute: bool = False,
    # ------------------------------------------------------------------
    # HSIC filter (sequential: top-level; tabular: per-partition inside generator)
    # ------------------------------------------------------------------
    use_hsic_filter: bool = False,
    hsic_target_sense: str = "A",
    hsic_threshold_multiplier: float = 1.0,
    # ------------------------------------------------------------------
    # Sequential-specific parameters
    # ------------------------------------------------------------------
    split_input_by_class: bool = False,
    select_test: bool = False,
    use_stratified_split: bool = False,
    stratified_metric: str = "rarity_last_value",
    train_ratio: float = 0.8,
    n_bins: int = 50,
    split_shuffle: bool = False,
    split_seed: int = 42,
    # ------------------------------------------------------------------
    # Tabular-specific parameters
    # ------------------------------------------------------------------
    target_sense: str = "A",          # 'A', 'B', or 'max'
    param_class: Optional[str] = None, # None/'all', 'read', 'set'
    dry_run: bool = False,
    slopes_path: Optional[str] = None,
    discovery_only: bool = False,
    consolidation_coverage: Optional[float] = None,
    min_partition_samples: Optional[int] = None,
    # ------------------------------------------------------------------
    # DEPRECATED (kept for backward compatibility — emit DeprecationWarning)
    # ------------------------------------------------------------------
    missing_threshold: Optional[float] = None,
    generate_sequence: Optional[bool] = None,
    generate_tabular: Optional[bool] = None,
):
    """
    Dyconex dataset assembly pipeline.

    Folder structure::

        data
            |__input              | process files here
            |__builds             |
                |__dataset_id     | must be created beforehand!
                    |__control    | control files here
                    |__output     | output files here

    Shared pipeline (both modes)
    ----------------------------
    1. ``assemble_raw``        – load and merge raw process CSV files
    2. ``process_raw``         – aggregate measurements, assign occurrence/steps,
                                  explode time components  (**no normalization**)
    3. ``compute_ist_slopes``  – compute IST regression slopes and classification
    4. ``filter_sparse_columns`` – remove parameter-columns with too many missing
                                  values (unified; replaces ``missing_threshold``
                                  in sequential and ``tabular_col_threshold`` in tabular)

    Sequential branch (``mode='sequential'``)
    -----------------------------------------
    5. ``filter_by_hsic``      – optional HSIC-based parameter filter
    6. ``generate_dataset``    – min-max normalize (saves ``denorm_map.json``),
                                  flatten to ``.npz`` tensors
    7. ``split_by_metric``     – optional stratified train/test split

    Tabular branch (``mode='tabular'``)
    ------------------------------------
    5. ``generate_tabular_dataset`` – internally:
          a. pivot to wide design matrix
          b. partition by exact non-null column pattern (separate sub-datasets)
          c. per partition: optional HSIC filter, min-max normalize, save outputs

    Parameters
    ----------
    dataset_id : str
        Dataset build identifier (folder name inside ``data/builds/``).
    mode : str
        Pipeline mode.  ``'sequential'`` (default) produces ``.npz`` sequence
        tensors.  ``'tabular'`` produces partitioned design matrices.
    col_missing_threshold : float
        A parameter-column is removed if it is absent for more than this
        percentage of process-chain groups.  Applies to both modes.
        Default 50 %.
    col_min_nonmissing_count : int
        Minimum number of groups that must have a non-null entry for a
        parameter-column to be retained.  Default 5.
    col_iterative_cleaning : bool
        Repeat column filtering until convergence.  Default False.
    col_filter_dry_run : bool
        Run column filter in report-only mode (writes log but does not
        modify ``df_input.parquet``).  Default False.
    grouping_method : str
        How to form process-chain group IDs (``'panel'`` or ``'column'``).
    grouping_column : str, optional
        Column name to use when ``grouping_method='column'``.
    selected_processes : list, optional
        Restrict assembly to these process names.  ``None`` loads all.
    debug : bool
        Assemble only a small slice of data (for testing).
    ist_fail_threshold : float
        IST FAIL threshold (raw slope).  Default 1/200 ≈ 0.005.
    ist_norm_method : str
        Slope normalization method: ``'minmax'`` (default) or ``'zscore'``.
    ist_force_recompute : bool
        Force recomputation of slopes even if cached.
    use_hsic_filter : bool
        Enable HSIC-based parameter filtering.
        *Sequential*: applied at the top level before ``generate_dataset``.
        *Tabular*: passed through to ``generate_tabular_dataset`` and applied
        per partition.
    hsic_target_sense : str
        Target sense for HSIC (``'A'`` default).
    hsic_threshold_multiplier : float
        Multiplier k for HSIC threshold.  Default 1.0.
    split_input_by_class : bool
        *Sequential only.*  Split input into Set/Read parameter tensors.
    select_test : bool
        *Sequential only.*  Export test indices from pre-selected IDs.
    use_stratified_split : bool
        *Sequential only.*  Perform stratified train/test split.
    stratified_metric : str
        Metric column used for stratification.
    train_ratio : float
        Train fraction for stratified split.  Default 0.8.
    n_bins : int
        Number of bins for stratification.  Default 50.
    split_shuffle : bool
        Shuffle within bins.  Default False.
    split_seed : int
        Random seed.  Default 42.
    target_sense : str
        *Tabular only.*  Target response variable: ``'A'``, ``'B'``, or ``'max'``.
    param_class : str, optional
        *Tabular only.*  Restrict design matrix to ``'read'``, ``'set'``, or
        ``None`` / ``'all'`` (default).
    dry_run : bool
        *Tabular only.*  Report-only mode for tabular generation.
    slopes_path : str, optional
        Custom path to ``slopes_summary.csv``.

    Notes
    -----
    - Normalization is **not** performed in ``process_raw`` — it is deferred
      to the mode-specific generation step so that normalization statistics
      are computed on the final clean population.
    - Normalization parameters are saved as ``denorm_map.json`` alongside
      each generated dataset for reproducible inverse-transform.
    - The deprecated ``missing_threshold``, ``generate_sequence``, and
      ``generate_tabular`` parameters are accepted for backward compatibility
      but will emit ``DeprecationWarning`` and have no effect.
    """
    import warnings

    # ------------------------------------------------------------------
    # Backward-compatibility warnings
    # ------------------------------------------------------------------
    if missing_threshold is not None:
        warnings.warn(
            "main(): 'missing_threshold' is deprecated. "
            "Use 'col_missing_threshold' instead.",
            DeprecationWarning, stacklevel=2,
        )
    if generate_sequence is not None:
        warnings.warn(
            "main(): 'generate_sequence' is deprecated. "
            "Use mode='sequential' or mode='tabular' instead.",
            DeprecationWarning, stacklevel=2,
        )
    if generate_tabular is not None:
        warnings.warn(
            "main(): 'generate_tabular' is deprecated. "
            "Use mode='tabular' instead.",
            DeprecationWarning, stacklevel=2,
        )

    # Validate mode
    if mode not in ("sequential", "tabular"):
        raise ValueError(f"mode must be 'sequential' or 'tabular'. Got: {mode!r}")

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    ROOT_DIR = dirname(dirname(abspath(__file__)))
    sys.path.append(ROOT_DIR)
    _, OUTPUT_DIR, _ = get_dirs(ROOT_DIR, dataset_id)

    if not exists(OUTPUT_DIR):
        makedirs(OUTPUT_DIR)

    log_filename = join(OUTPUT_DIR, "process_chain_build.log")
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.info(f"Pipeline started | mode='{mode}' | dataset_id='{dataset_id}'")

    # ==================================================================
    # SHARED BACKBONE
    # ==================================================================

    logging.info("Assembling raw process dataframe...")
    assemble_raw(
        dataset_id=dataset_id,
        grouping_method=grouping_method,
        grouping_column=grouping_column,
        selected_processes=selected_processes,
        debug=debug,
    )
    logging.info("Raw dataframe assembly complete")

    logging.info("Processing raw dataframe (aggregate + occurrence/steps; no normalization)...")
    process_raw(dataset_id=dataset_id)
    logging.info("Raw dataframe processing complete")

    logging.info("Computing IST slopes and classifying pass/fail...")
    ist_summary = compute_ist_slopes(
        dataset_id=dataset_id,
        fail_threshold=ist_fail_threshold,
        norm_method=ist_norm_method,
        force_recompute=ist_force_recompute,
    )
    logging.info(
        f"IST slopes complete: {ist_summary['n_samples']} samples | "
        f"fail_threshold={ist_summary['fail_threshold']:.5f} | "
        + " | ".join(
            f"{k}={v}"
            for k, v in ist_summary.items()
            if k.startswith("n_fail_")
        )
    )

    if col_missing_threshold is not None or col_min_nonmissing_count is not None:
        logging.info(
            f"Filtering sparse columns "
            f"(threshold={col_missing_threshold}%, "
            f"min_count={col_min_nonmissing_count}, "
            f"iterative={col_iterative_cleaning}) ..."
        )
        col_filter_summary = filter_sparse_columns(
            dataset_id=dataset_id,
            col_missing_threshold=col_missing_threshold,
            min_nonmissing_count=col_min_nonmissing_count,
            iterative=col_iterative_cleaning,
            dry_run=col_filter_dry_run,
        )
        logging.info(
            f"Column filter complete: "
            f"{col_filter_summary['n_cols_before']} -> {col_filter_summary['n_cols_after']} cols "
            f"({col_filter_summary['n_cols_excluded']} excluded)"
        )
    else:
        logging.info(
            "Skipping filter_sparse_columns: "
            "both col_missing_threshold and col_min_nonmissing_count are None"
        )

    # ==================================================================
    # SEQUENTIAL BRANCH
    # ==================================================================
    if mode == "sequential":
        logging.info("--- SEQUENTIAL mode ---")

        if use_hsic_filter:
            logging.info("Filtering parameters by HSIC independence criterion...")
            logging.info(f"  Target sense: {hsic_target_sense}")
            logging.info(f"  Threshold multiplier (k): {hsic_threshold_multiplier}")
            filter_info = filter_by_hsic(
                dataset_id=dataset_id,
                target_sense=hsic_target_sense,
                threshold_multiplier=hsic_threshold_multiplier,
            )
            logging.info(
                f"HSIC filtering complete: {filter_info['n_params_filtered']} parameters removed | "
                f"baseline={filter_info['baseline_name']} (HSIC={filter_info['baseline_value']:.6f}) | "
                f"threshold={filter_info['threshold']:.6f} | "
                f"retained={filter_info['n_params_after']}/{filter_info['n_params_before']}"
            )

        logging.info("Generating sequence dataset (normalize + flatten)...")
        generate_dataset(dataset_id=dataset_id, split_input_by_class=split_input_by_class)
        logging.info("Sequence dataset generation complete")

        if select_test:
            logging.info("Exporting selected indices...")
            get_idx_from_id(
                dataset_id=dataset_id,
                id_sel_filename="selected_id.npy",
                idx_sel_filename="test_ds_idx.npy",
            )
            logging.info("Index selection complete")

        if use_stratified_split:
            logging.info("Performing stratified split...")
            split_by_metric(
                dataset_id=dataset_id,
                metric_column=stratified_metric,
                train_ratio=train_ratio,
                n_bins=n_bins,
                shuffle=split_shuffle,
                seed=split_seed,
            )
            logging.info("Stratified split complete")

    # ==================================================================
    # TABULAR BRANCH
    # ==================================================================
    elif mode == "tabular":
        logging.info("--- TABULAR mode ---")
        mode_label = "dry-run" if dry_run else "full"
        logging.info(
            f"Generating tabular dataset ({mode_label}, "
            f"sense='{target_sense}', "
            f"param_class={param_class!r}, "
            f"use_hsic={use_hsic_filter}) ..."
        )
        tabular_summary = generate_tabular_dataset(
            dataset_id=dataset_id,
            target_sense=target_sense,
            param_class=param_class,
            dry_run=dry_run,
            slopes_path=slopes_path,
            use_hsic=use_hsic_filter,
            hsic_target_sense=hsic_target_sense,
            hsic_threshold_multiplier=hsic_threshold_multiplier,
            discovery_only=discovery_only,
            consolidation_coverage=consolidation_coverage,
            min_partition_samples=min_partition_samples,
        )
        n_part = tabular_summary.get("n_partitions", "?")
        logging.info(
            f"Tabular dataset generation complete: "
            f"{n_part} partition(s) | "
            f"output: {tabular_summary.get('output_dir', '?')}"
        )


if __name__ == "__main__":

    dataset_case = 3

    # Dataset for proT paper
    if dataset_case == 1:
        pass

    # SET vs READ dataset for Jeffry and Matteo
    elif dataset_case == 2:
        main(
            dataset_id="dyconex_SX_MuMi_260302",
            mode="sequential",
            col_missing_threshold=30.0,
            use_stratified_split=True,
            use_hsic_filter=True,
            selected_processes=["Multibond", "Microetch"],
            split_input_by_class=True,
        )

    # Tabular dataset
    elif dataset_case == 3:
        print("Generating option 4 — Phase 2+3 (consolidate + generate)")
        main(
            dataset_id="dyconex_tabular_260518",
            mode="tabular",
            col_missing_threshold=None,
            col_min_nonmissing_count=None,
            selected_processes=None,
            use_hsic_filter=True,
            hsic_target_sense="B",
            hsic_threshold_multiplier=2,
            target_sense="B",
            param_class="read",
            discovery_only=False,
            consolidation_coverage=0.9,
            min_partition_samples=50,
            dry_run=False,
            debug=False,
        )
