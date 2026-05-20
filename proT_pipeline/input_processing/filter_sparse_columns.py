"""
Sparse-column filtering for proT_pipeline.

This module provides a unified column-level sparsity filter that operates on
the long-format ``df_input.parquet`` produced by ``process_raw()``.

It replaces two previously separate mechanisms:
- ``filter_vars_max_missing()`` inside ``process_raw.py``  (sequential path)
- ``col_missing_threshold`` cleaning inside ``generate_tabular_dataset.py``  (tabular path)

The filter removes every (process, occurrence, step, variable) combination —
referred to as a "parameter column" — for which:
  - the fraction of process-chain groups that have no entry > ``col_missing_threshold``%
  - OR the absolute number of non-missing groups < ``min_nonmissing_count``

Column labels follow the same ``Process_occ_step_variable`` convention used by
the tabular pivot, so the exclusion log is directly comparable to the tabular
exclusion log.

Typical usage
-------------
After ``process_raw()`` and before ``filter_by_hsic()`` / ``generate_dataset()``::

    from proT_pipeline.input_processing.filter_sparse_columns import filter_sparse_columns
    info = filter_sparse_columns(dataset_id="my_dataset", col_missing_threshold=50.0)

Outputs (all written to ``output/``)
--------------------------------------
``df_input.parquet``
    Overwritten in-place with sparse columns removed.
``df_input_prefilter.parquet``
    Backup of the un-filtered data (written once; never overwritten).
``column_filter_log.json``
    Per-column filtering decision: label, pct_missing, nonmissing_count,
    excluded (bool), reasons.
``column_filter_summary.json``
    High-level summary: n_cols_before, n_cols_after, n_cols_excluded,
    threshold settings, timestamp.
"""

import json
import logging
import warnings
from datetime import datetime
from os.path import abspath, dirname, exists, join
from typing import Any, Dict, List, Optional, Tuple
import sys

import pandas as pd

ROOT_DIR = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(ROOT_DIR)

from proT_pipeline.labels import (
    get_dirs,
    get_root_dir,
    trans_df_input,
    trans_group_id,
    trans_occurrence_label,
    trans_process_label,
    trans_step_label,
    trans_variable_label,
    trans_value_label,
)

logger = logging.getLogger(__name__)

# Reason codes
_REASON_MISSING_PCT = "col_missing_pct_exceeds_threshold"
_REASON_MIN_COUNT   = "col_min_nonmissing_count_not_reached"

# Backup filename written alongside df_input.parquet
_BACKUP_FILENAME = "df_input_prefilter.parquet"
_LOG_FILENAME = "column_filter_log.json"
_SUMMARY_FILENAME = "column_filter_summary.json"


# ============================================================================
# COLUMN LABEL HELPER (matches generate_tabular_dataset convention)
# ============================================================================

def _make_col_label(process: str, occurrence: Any, step: Any, variable: str) -> str:
    """Build ``Process_occ_step_variable`` column label (int-cast occ/step)."""
    occ_str  = str(int(occurrence)) if pd.notna(occurrence) else "NA"
    step_str = str(int(step))       if pd.notna(step)       else "NA"
    return f"{process}_{occ_str}_{step_str}_{variable}"


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def filter_sparse_columns(
    dataset_id: str,
    col_missing_threshold: float = 50.0,
    min_nonmissing_count: int = 5,
    iterative: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Remove sparse parameter-columns from ``df_input.parquet``.

    Parameters
    ----------
    dataset_id : str
        Dataset identifier (folder name inside ``data/builds/``).
    col_missing_threshold : float
        A parameter-column is excluded if the fraction of process-chain
        groups that have *no entry* for it exceeds this percentage.
        Default 50 %.
    min_nonmissing_count : int
        A parameter-column is also excluded if the absolute number of
        groups that DO have an entry is below this count.
        Default 5.
    iterative : bool
        If ``True``, repeat the filter until no more columns are removed
        (convergence).  Default ``False`` (single pass).
    dry_run : bool
        If ``True``, compute and log what *would* be removed but do not
        modify ``df_input.parquet``.  The log files ARE written even in
        dry-run mode.  Default ``False``.

    Returns
    -------
    dict
        Summary with keys:
        ``n_cols_before``, ``n_cols_after``, ``n_cols_excluded``,
        ``n_rows_before``, ``n_rows_after``,
        ``col_missing_threshold``, ``min_nonmissing_count``,
        ``dry_run``, ``output_path``.

    Raises
    ------
    FileNotFoundError
        If ``df_input.parquet`` does not exist (run ``process_raw()`` first).
    """
    ROOT_DIR = get_root_dir()
    _, OUTPUT_DIR, _ = get_dirs(ROOT_DIR, dataset_id)

    df_input_path    = join(OUTPUT_DIR, trans_df_input)
    backup_path      = join(OUTPUT_DIR, _BACKUP_FILENAME)
    log_path         = join(OUTPUT_DIR, _LOG_FILENAME)
    summary_path     = join(OUTPUT_DIR, _SUMMARY_FILENAME)

    if not exists(df_input_path):
        raise FileNotFoundError(
            f"Processed input file not found: {df_input_path}\n"
            "Run process_raw() first to create this file."
        )

    df = pd.read_parquet(df_input_path)
    n_rows_before = len(df)
    logger.info(
        f"[filter_sparse_columns] Loaded df_input: "
        f"{n_rows_before:,} rows, "
        f"{df[trans_group_id].nunique():,} unique groups"
    )

    # Write backup once (do not overwrite an existing backup)
    if not dry_run and not exists(backup_path):
        df.to_parquet(backup_path)
        logger.info(f"[filter_sparse_columns] Backup saved: {backup_path}")

    # ------------------------------------------------------------------
    # Build column labels
    # ------------------------------------------------------------------
    df = df.copy()
    df["_col_label"] = df.apply(
        lambda row: _make_col_label(
            row[trans_process_label],
            row[trans_occurrence_label],
            row[trans_step_label],
            row[trans_variable_label],
        ),
        axis=1,
    )

    all_groups = set(df[trans_group_id].unique())
    n_groups   = len(all_groups)

    # ------------------------------------------------------------------
    # Iterative filter loop
    # ------------------------------------------------------------------
    exclusion_log: List[Dict] = []
    col_labels_before = set(df["_col_label"].unique())
    pass_num = 0

    while True:
        pass_num += 1
        cols_to_drop, pass_log = _one_pass(
            df=df,
            all_groups=all_groups,
            n_groups=n_groups,
            col_missing_threshold=col_missing_threshold,
            min_nonmissing_count=min_nonmissing_count,
            pass_number=pass_num,
        )
        exclusion_log.extend(pass_log)

        if not cols_to_drop or dry_run:
            break

        df = df[~df["_col_label"].isin(cols_to_drop)].copy()
        logger.info(f"  Pass {pass_num}: removed {len(cols_to_drop)} columns")

        if not iterative:
            break

    # ------------------------------------------------------------------
    # Compute final stats
    # ------------------------------------------------------------------
    col_labels_after   = set(df["_col_label"].unique()) if not dry_run else (col_labels_before - {e["col_label"] for e in exclusion_log})
    n_cols_before      = len(col_labels_before)
    n_cols_after       = len(col_labels_after)
    n_cols_excluded    = n_cols_before - n_cols_after

    # Remove helper column before saving
    df_clean = df.drop(columns=["_col_label"])
    n_rows_after = len(df_clean)

    logger.info(
        f"[filter_sparse_columns] Result: "
        f"{n_cols_before} cols → {n_cols_after} cols "
        f"({n_cols_excluded} excluded) | "
        f"{n_rows_before:,} rows → {n_rows_after:,} rows"
    )

    # ------------------------------------------------------------------
    # Save outputs
    # ------------------------------------------------------------------
    # Log is always written (even in dry_run)
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(exclusion_log, f, indent=2, default=str)
    logger.info(f"[filter_sparse_columns] Saved {log_path}")

    summary = {
        "timestamp": datetime.now().isoformat(),
        "dataset_id": dataset_id,
        "col_missing_threshold": col_missing_threshold,
        "min_nonmissing_count": min_nonmissing_count,
        "iterative": iterative,
        "dry_run": dry_run,
        "n_groups": n_groups,
        "n_cols_before": n_cols_before,
        "n_cols_after": n_cols_after,
        "n_cols_excluded": n_cols_excluded,
        "n_rows_before": n_rows_before,
        "n_rows_after": n_rows_after,
        "output_path": df_input_path,
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info(f"[filter_sparse_columns] Saved {summary_path}")

    if not dry_run:
        df_clean.to_parquet(df_input_path)
        logger.info(f"[filter_sparse_columns] Updated {df_input_path}")

    return summary


# ============================================================================
# PRIVATE HELPERS
# ============================================================================

def _one_pass(
    df: pd.DataFrame,
    all_groups: set,
    n_groups: int,
    col_missing_threshold: float,
    min_nonmissing_count: int,
    pass_number: int,
) -> Tuple[List[str], List[Dict]]:
    """
    One filtering pass over the long-format DataFrame.

    Returns
    -------
    cols_to_drop : list of str
        Column labels that should be removed.
    pass_log : list of dict
        Per-column log entries for excluded columns.
    """
    # Count how many distinct groups have at least one entry for each col
    groups_per_col: pd.Series = (
        df.groupby("_col_label")[trans_group_id]
        .nunique()
    )

    cols_to_drop: List[str] = []
    pass_log: List[Dict] = []

    for col_label, n_present in groups_per_col.items():
        pct_missing = (1.0 - n_present / n_groups) * 100.0
        reasons: List[str] = []

        if pct_missing > col_missing_threshold:
            reasons.append(_REASON_MISSING_PCT)
        if n_present < min_nonmissing_count:
            reasons.append(_REASON_MIN_COUNT)

        if reasons:
            cols_to_drop.append(col_label)
            entry = {
                "col_label": str(col_label),
                "pct_missing": round(pct_missing, 4),
                "nonmissing_count": int(n_present),
                "reasons": reasons,
                "cleaning_pass": pass_number,
            }
            pass_log.append(entry)
            logger.debug(
                f"  [pass {pass_number}] EXCLUDE col='{col_label}' | "
                f"{'; '.join(reasons)} | "
                f"missing={pct_missing:.1f}%  n_valid={n_present}"
            )

    return cols_to_drop, pass_log


# ============================================================================
# STANDALONE ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Filter sparse parameter-columns from df_input.parquet."
    )
    parser.add_argument("dataset_id", help="Dataset identifier (folder in data/builds/)")
    parser.add_argument(
        "--col_threshold", type=float, default=50.0,
        help="Column missing-%% threshold (default: 50)"
    )
    parser.add_argument(
        "--min_count", type=int, default=5,
        help="Minimum non-missing group count (default: 5)"
    )
    parser.add_argument("--iterative", action="store_true", help="Iterative cleaning")
    parser.add_argument("--dry_run", action="store_true", help="Report only, do not save")

    args = parser.parse_args()

    info = filter_sparse_columns(
        dataset_id=args.dataset_id,
        col_missing_threshold=args.col_threshold,
        min_nonmissing_count=args.min_count,
        iterative=args.iterative,
        dry_run=args.dry_run,
    )
    print("\nColumn filter summary:")
    for k, v in info.items():
        print(f"  {k}: {v}")
