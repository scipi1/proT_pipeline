"""
Tabular dataset generation for proT_pipeline.

This module creates partitioned tabular design matrices
(rows = process chains, columns = parameters) from the processed input data.

Pipeline
--------
1. Load ``df_input.parquet`` (raw, un-normalized) + ``slopes_summary.csv``.
2. Apply optional ``param_class`` filter (read / set / all).
3. Pivot long → wide using raw ``trans_value_label`` values.
4. Partition by **exact non-null column pattern**: rows that share the same
   set of non-null columns form one partition.  Each partition is saved in a
   separate sub-folder so that downstream analysis never mixes incompatible
   column structures.
5. Per partition:
   a. Optional HSIC filtering via ``filter_dataframe_by_hsic``.
   b. Min-max normalization per variable → ``denorm_map.json`` saved alongside
      the dataset for reproducible inverse-transform.
   c. Temporal column ordering (median timestamp) and row ordering
      (minimum timestamp, oldest first).
   d. Save design matrix, target, vocabularies, order dicts, build summary.

This design intentionally does NOT perform row-level sparsity cleaning
(rows are partitioned instead of discarded) and does NOT duplicate the
column-level sparsity filter already applied by ``filter_sparse_columns()``.

Typical usage::

    from proT_pipeline.input_processing.generate_tabular_dataset import generate_tabular_dataset
    summary = generate_tabular_dataset(
        dataset_id="my_dataset",
        target_sense="A",
        use_hsic=True,
    )

All outputs are written to ``output/tabular_dataset/`` inside the dataset build folder.
Each partition gets its own sub-folder named ``partition_{n_rows}rows_{n_cols}cols_{hash8}/``.
A ``partition_overview.csv`` at the top level summarises all partitions in one flat table.
"""

import csv as csv_module
import hashlib
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from os import makedirs
from os.path import abspath, dirname, exists, join
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

ROOT_DIR = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(ROOT_DIR)

from proT_pipeline.labels import (
    get_dirs,
    get_root_dir,
    trans_class_label,
    trans_date_label,
    trans_design_version_label,
    trans_df_input,
    trans_group_id,
    trans_occurrence_label,
    trans_process_label,
    trans_step_label,
    trans_value_label,
    trans_variable_label,
)
from proT_pipeline.utils import safe_read_csv
from proT_pipeline.input_processing.filter_by_hsic import filter_dataframe_by_hsic
from proT_pipeline.input_processing.consolidate_partitions import (
    consolidate as _consolidate_partitions,
    preview_consolidation as _preview_consolidation,
)

# ============================================================================
# CONSTANTS
# ============================================================================

TABULAR_OUTPUT_SUBDIR = "tabular_dataset"
SLOPES_FILENAME = "slopes_summary.csv"
SLOPES_SUBDIR = "hsic_analysis"          # legacy fallback

_COL_SLOPE_A = "slope_delta_A_norm"
_COL_SLOPE_B = "slope_delta_B_norm"

_PARAM_CLASS_MAP = {"read": 1, "set": 2}
_VALID_PARAM_CLASSES = frozenset({"all", "read", "set", None})

logger = logging.getLogger(__name__)


# ============================================================================
# PRIVATE HELPERS – COLUMN LABELS
# ============================================================================

def _make_col_label(process: str, occurrence: Any, step: Any, variable: str) -> str:
    occ_str  = str(int(occurrence)) if pd.notna(occurrence) else "NA"
    step_str = str(int(step))       if pd.notna(step)       else "NA"
    return f"{process}_{occ_str}_{step_str}_{variable}"


def _build_col_labels(df: pd.DataFrame) -> Tuple[pd.Series, Dict[str, Dict]]:
    labels = df.apply(
        lambda row: _make_col_label(
            row[trans_process_label], row[trans_occurrence_label],
            row[trans_step_label],    row[trans_variable_label],
        ), axis=1,
    )
    key_cols = [trans_process_label, trans_occurrence_label, trans_step_label, trans_variable_label]
    tmp = df[key_cols].copy()
    tmp["_col_label"] = labels
    col_decomp: Dict[str, Dict] = {}
    for _, row in tmp.drop_duplicates(subset="_col_label").iterrows():
        lbl = row["_col_label"]
        col_decomp[lbl] = {
            "process":    str(row[trans_process_label]),
            "occurrence": int(row[trans_occurrence_label]) if pd.notna(row[trans_occurrence_label]) else None,
            "step":       int(row[trans_step_label])       if pd.notna(row[trans_step_label])       else None,
            "variable":   str(row[trans_variable_label]),
        }
    return labels, col_decomp


# ============================================================================
# PRIVATE HELPERS – SLOPES / TARGET
# ============================================================================

def _load_slopes(output_dir: str, slopes_path: Optional[str]) -> pd.DataFrame:
    if slopes_path is None:
        backbone = join(output_dir, SLOPES_FILENAME)
        legacy   = join(output_dir, SLOPES_SUBDIR, SLOPES_FILENAME)
        if exists(backbone):
            slopes_path = backbone
        elif exists(legacy):
            slopes_path = legacy
        else:
            raise FileNotFoundError(
                f"slopes_summary.csv not found.\n"
                f"  Backbone: {backbone}\n  Legacy: {legacy}\n"
                "Run compute_ist_slopes() first."
            )
    elif not exists(slopes_path):
        raise FileNotFoundError(f"slopes_summary.csv not found: {slopes_path}")

    df = safe_read_csv(slopes_path)
    missing = [c for c in [trans_group_id, _COL_SLOPE_A, _COL_SLOPE_B] if c not in df.columns]
    if missing:
        raise ValueError(f"slopes_summary.csv missing columns: {missing}")
    return df


def _compute_target_series(df_slopes: pd.DataFrame, sense: str) -> pd.Series:
    s = df_slopes.set_index(trans_group_id)
    if sense == "A":
        return s[_COL_SLOPE_A].rename("target")
    elif sense == "B":
        return s[_COL_SLOPE_B].rename("target")
    elif sense == "max":
        return s[[_COL_SLOPE_A, _COL_SLOPE_B]].max(axis=1).rename("target")
    raise ValueError(f"target_sense must be 'A', 'B', or 'max'. Got: {sense!r}")


# ============================================================================
# PRIVATE HELPERS – PARTITIONING
# ============================================================================

def _partition_by_column_pattern(
    value_matrix: pd.DataFrame,
) -> Dict[str, List]:
    """
    Group row indices (group IDs) by their exact set of non-null columns.

    Returns
    -------
    dict
        ``{partition_id: [group_id, ...]}``
        ``partition_id`` is the first 12 characters of the MD5 hash of the
        sorted frozenset of non-null column names (deterministic).
    """
    partitions: Dict[str, List] = defaultdict(list)
    for group in value_matrix.index:
        non_null_cols = frozenset(
            col for col in value_matrix.columns
            if pd.notna(value_matrix.loc[group, col])
        )
        pattern_key = ",".join(sorted(non_null_cols))
        hash_suffix = hashlib.md5(pattern_key.encode()).hexdigest()[:8]
        part_id = f"{len(non_null_cols)}cols_{hash_suffix}"
        partitions[part_id].append(group)
    return dict(partitions)


# ============================================================================
# PRIVATE HELPERS – NORMALIZATION
# ============================================================================

def _normalize_partition(
    df_long: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, Dict]]:
    """
    Apply per-variable min-max normalization to a long-format partition.

    Returns the normalized DataFrame (with ``trans_value_label`` updated
    to normalized values in-place copy) and the denorm map dict.

    Note: we normalize in-place on ``trans_value_label`` because the tabular
    pivot uses raw values.  A separate ``_norm`` column is NOT needed here —
    the design matrix is written AFTER normalization, so it already holds
    normalized values.
    """
    df = df_long.copy()
    stats = df.groupby(trans_variable_label)[trans_value_label].agg(["min", "max"])
    denorm_map: Dict[str, Dict] = {
        str(var): {"min": float(row["min"]), "max": float(row["max"])}
        for var, row in stats.iterrows()
    }
    v_min   = df[trans_variable_label].map(stats["min"])
    v_max   = df[trans_variable_label].map(stats["max"])
    v_range = (v_max - v_min).clip(lower=1e-12)
    df[trans_value_label] = (df[trans_value_label] - v_min) / v_range
    return df, denorm_map


# ============================================================================
# PRIVATE HELPERS – TEMPORAL ORDERING
# ============================================================================

def _order_columns_by_timestamp(
    matrix: pd.DataFrame, ts_matrix: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, Dict]]:
    nat_sentinel = pd.Timestamp.max
    col_median: Dict[str, Any] = {}
    for col in matrix.columns:
        if col in ts_matrix.columns:
            valid = ts_matrix[col].dropna()
            col_median[col] = valid.median() if len(valid) > 0 else pd.NaT
        else:
            col_median[col] = pd.NaT

    def _key(col):
        ts = col_median[col]
        return (ts if pd.notna(ts) else nat_sentinel, col)

    sorted_cols = sorted(matrix.columns.tolist(), key=_key)
    ts_to_cols: Dict = defaultdict(list)
    for col in matrix.columns:
        ts = col_median[col]
        k = ts.isoformat() if pd.notna(ts) else None
        ts_to_cols[k].append(col)
    tied = {k for k, v in ts_to_cols.items() if k is not None and len(v) > 1}

    col_order: Dict[str, Dict] = {}
    for rank, col in enumerate(sorted_cols):
        ts = col_median[col]
        ts_str = ts.isoformat() if pd.notna(ts) else None
        col_order[col] = {"rank": rank, "median_timestamp": ts_str, "conflict": ts_str in tied}
    return matrix[sorted_cols], col_order


def _order_rows_by_timestamp(
    matrix: pd.DataFrame, ts_matrix: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, Dict]]:
    common_cols = [c for c in matrix.columns if c in ts_matrix.columns]
    # Use reindex instead of .loc so that groups present in value_matrix
    # (e.g. imputed rows from consolidation) but absent from ts_matrix
    # (because HSIC removed all their long-format rows) get NaT rather
    # than raising a KeyError.
    ts_sub = ts_matrix.reindex(index=matrix.index, columns=common_cols)
    row_min_ts = ts_sub.min(axis=1)
    sentinel = pd.Timestamp.max
    sort_keys = row_min_ts.copy()
    sort_keys[sort_keys.isna()] = sentinel
    sorted_groups = sort_keys.sort_values().index.tolist()
    matrix = matrix.loc[sorted_groups]
    ts_sub  = ts_sub.loc[sorted_groups]

    conflicts: Dict[str, Any] = {}
    for i in range(len(sorted_groups) - 1):
        g_cur, g_nxt = sorted_groups[i], sorted_groups[i + 1]
        bad_cols = [
            c for c in common_cols
            if pd.notna(ts_sub.loc[g_cur, c]) and pd.notna(ts_sub.loc[g_nxt, c])
            and ts_sub.loc[g_cur, c] > ts_sub.loc[g_nxt, c]
        ]
        if bad_cols:
            conflicts[str(g_nxt)] = {"conflicting_with_previous": str(g_cur),
                                      "columns": bad_cols}

    row_order: Dict[str, Dict] = {}
    for rank, group in enumerate(sorted_groups):
        ts = row_min_ts.get(group)
        row_order[str(group)] = {
            "sorted_row_index": rank,
            "min_timestamp": ts.isoformat() if pd.notna(ts) else None,
            "ordering_conflict": conflicts.get(str(group)),
        }
    return matrix, row_order


# ============================================================================
# PRIVATE HELPERS – I/O
# ============================================================================

def _save_json(obj: Any, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)


def _normalize_value_matrix(
    matrix: pd.DataFrame,
    col_decomp: Dict[str, Dict],
) -> Tuple[pd.DataFrame, Dict[str, Dict]]:
    """Min-max normalize a wide matrix per variable (variable = col_decomp[col]['variable']).

    Used when a consolidated (pre-imputed) value_matrix is provided directly
    to ``_process_partition_inner`` instead of going through ``_normalize_partition``.
    """
    df = matrix.copy()
    var_to_cols: Dict[str, List] = defaultdict(list)
    for col in df.columns:
        var = col_decomp.get(col, {}).get("variable", col)
        var_to_cols[str(var)].append(col)

    denorm_map: Dict[str, Dict] = {}
    for var, cols in var_to_cols.items():
        all_vals = df[cols].values.flatten()
        finite = all_vals[~np.isnan(all_vals)]
        if len(finite) == 0:
            continue
        lo, hi = float(np.nanmin(finite)), float(np.nanmax(finite))
        denorm_map[var] = {"min": lo, "max": hi}
        v_range = max(hi - lo, 1e-12)
        for col in cols:
            df[col] = (df[col] - lo) / v_range
    return df, denorm_map


def _global_column_order(ts_matrix: pd.DataFrame) -> Dict[str, int]:
    """Return ``{col: rank}`` sorted by global median timestamp (ascending).

    Used in discovery mode to order columns in ``partition_overview.csv``
    without running per-partition temporal ordering.
    """
    nat_sentinel = pd.Timestamp.max
    col_medians: Dict[str, Any] = {}
    for col in ts_matrix.columns:
        valid = ts_matrix[col].dropna()
        col_medians[col] = valid.median() if len(valid) > 0 else pd.NaT

    sorted_cols = sorted(
        ts_matrix.columns.tolist(),
        key=lambda c: (
            col_medians[c] if pd.notna(col_medians[c]) else nat_sentinel,
            c,
        ),
    )
    return {col: rank for rank, col in enumerate(sorted_cols)}


# ============================================================================
# PER-PARTITION PROCESSING
# ============================================================================

def _process_partition(
    partition_id: str,
    groups: List,
    df_long_all: pd.DataFrame,
    df_slopes: pd.DataFrame,
    col_decomp: Dict[str, Dict],
    ts_matrix_all: pd.DataFrame,
    value_matrix_all: pd.DataFrame,
    target_sense: str,
    tabular_dir: str,
    use_hsic: bool,
    hsic_target_sense: str,
    hsic_threshold_multiplier: float,
    dry_run: bool,
    prebuilt_value_matrix: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    """
    Process a single partition: HSIC filter → normalize → order → save.
    Returns a per-partition summary dict.

    Parameters
    ----------
    prebuilt_value_matrix : pd.DataFrame, optional
        Pre-imputed wide matrix (indexed by group_id, columns = col_labels)
        produced by ``consolidate_partitions``.  When provided, this matrix
        is used in place of re-pivoting from ``df_long_all``.
    """
    part_dir = join(tabular_dir, f"partition_{partition_id}")
    if not exists(part_dir):
        makedirs(part_dir)

    log_path = join(part_dir, "partition_build.log")
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)

    try:
        return _process_partition_inner(
            partition_id=partition_id,
            groups=groups,
            df_long_all=df_long_all,
            df_slopes=df_slopes,
            col_decomp=col_decomp,
            ts_matrix_all=ts_matrix_all,
            value_matrix_all=value_matrix_all,
            target_sense=target_sense,
            part_dir=part_dir,
            use_hsic=use_hsic,
            hsic_target_sense=hsic_target_sense,
            hsic_threshold_multiplier=hsic_threshold_multiplier,
            dry_run=dry_run,
            prebuilt_value_matrix=prebuilt_value_matrix,
        )
    finally:
        logger.removeHandler(fh)
        fh.close()


def _process_partition_inner(
    partition_id: str,
    groups: List,
    df_long_all: pd.DataFrame,
    df_slopes: pd.DataFrame,
    col_decomp: Dict[str, Dict],
    ts_matrix_all: pd.DataFrame,
    value_matrix_all: pd.DataFrame,
    target_sense: str,
    part_dir: str,
    use_hsic: bool,
    hsic_target_sense: str,
    hsic_threshold_multiplier: float,
    dry_run: bool,
    prebuilt_value_matrix: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:

    logger.info(f"[partition {partition_id}] {len(groups)} groups")

    # Slice the long-format data to this partition's groups
    df_long = df_long_all[df_long_all[trans_group_id].isin(groups)].copy()

    # ------------------------------------------------------------------
    # HSIC filter (per-partition, optional)
    # ------------------------------------------------------------------
    hsic_info: Optional[Dict] = None
    if use_hsic:
        slopes_indexed = df_slopes.set_index(trans_group_id)
        target_col = (
            _COL_SLOPE_A if hsic_target_sense == "A"
            else _COL_SLOPE_B if hsic_target_sense == "B"
            else None
        )
        if target_col is None:
            # max sense: use slope A for HSIC (typical choice)
            target_col = _COL_SLOPE_A
            logger.info("[HSIC] target_sense='max' → using slope_A for HSIC")

        common = [g for g in groups if g in slopes_indexed.index]
        if len(common) >= 5:
            target_ser = slopes_indexed.loc[common, target_col].rename("target")
            df_long, hsic_info = filter_dataframe_by_hsic(
                df_input=df_long,
                target_series=target_ser,
                threshold_multiplier=hsic_threshold_multiplier,
            )
            logger.info(
                f"[partition {partition_id}] HSIC: retained "
                f"{hsic_info['n_params_after']}/{hsic_info['n_params_before']} params"
            )
            if not dry_run:
                _save_json(hsic_info, join(part_dir, "hsic_filter_info.json"))
        else:
            logger.warning(
                f"[partition {partition_id}] HSIC skipped: "
                f"only {len(common)} groups have target values (min 5 required)"
            )

    # ------------------------------------------------------------------
    # Build _col_label on df_long (always needed: timestamps + HSIC column
    # set).  Then normalize + assemble value_matrix.
    # ------------------------------------------------------------------
    df_long["_col_label"] = df_long.apply(
        lambda row: _make_col_label(
            row[trans_process_label], row[trans_occurrence_label],
            row[trans_step_label],    row[trans_variable_label],
        ), axis=1,
    )
    # retained_cols reflects any HSIC column filter applied above
    retained_cols = set(df_long["_col_label"].unique())

    if prebuilt_value_matrix is not None:
        # Consolidated partition: use the pre-imputed wide matrix.
        # Filter to columns retained after HSIC, then normalize per-variable.
        available_cols = [c for c in prebuilt_value_matrix.columns if c in retained_cols]
        value_matrix, denorm_map = _normalize_value_matrix(
            prebuilt_value_matrix.loc[groups, available_cols].copy(),
            col_decomp,
        )
        value_matrix.columns.name = None
        logger.info(
            f"[partition {partition_id}] Pre-imputed matrix: "
            f"{len(available_cols)} col(s), {len(denorm_map)} variable(s) normalized"
        )
    else:
        # Standard path: normalize long-format then pivot to wide.
        df_long, denorm_map = _normalize_partition(df_long)
        value_matrix = (
            df_long
            .groupby([trans_group_id, "_col_label"])[trans_value_label]
            .mean()
            .unstack()
        )
        value_matrix.columns.name = None
        logger.info(f"[partition {partition_id}] Normalized {len(denorm_map)} variables")

    if not dry_run:
        _save_json(denorm_map, join(part_dir, "denorm_map.json"))

    has_date = trans_date_label in df_long.columns
    if has_date:
        ts_matrix: pd.DataFrame = (
            df_long
            .groupby([trans_group_id, "_col_label"])[trans_date_label]
            .first()
            .unstack()
        )
        ts_matrix.columns.name = None
        for col in ts_matrix.columns:
            ts_matrix[col] = pd.to_datetime(ts_matrix[col], errors="coerce")
    else:
        ts_matrix = pd.DataFrame(
            index=value_matrix.index, columns=value_matrix.columns,
            dtype="datetime64[ns]",
        )

    logger.info(
        f"[partition {partition_id}] Matrix after HSIC+norm: "
        f"{value_matrix.shape[0]} rows × {value_matrix.shape[1]} cols"
    )

    # ------------------------------------------------------------------
    # Column temporal ordering
    # ------------------------------------------------------------------
    if has_date and not ts_matrix.empty:
        value_matrix, col_order = _order_columns_by_timestamp(value_matrix, ts_matrix)
        ts_matrix = ts_matrix[[c for c in value_matrix.columns if c in ts_matrix.columns]]
    else:
        col_order = {
            col: {"rank": i, "median_timestamp": None, "conflict": False}
            for i, col in enumerate(value_matrix.columns)
        }

    # ------------------------------------------------------------------
    # Row temporal ordering
    # ------------------------------------------------------------------
    if has_date and not ts_matrix.empty:
        value_matrix, row_order = _order_rows_by_timestamp(value_matrix, ts_matrix)
    else:
        row_order = {
            str(g): {"sorted_row_index": i, "min_timestamp": None, "ordering_conflict": None}
            for i, g in enumerate(value_matrix.index)
        }

    # ------------------------------------------------------------------
    # Build vocabularies
    # ------------------------------------------------------------------
    col_vocab: Dict[int, str] = {i: col for i, col in enumerate(value_matrix.columns)}
    row_vocab: Dict[int, str] = {i: str(g) for i, g in enumerate(value_matrix.index)}

    col_vocab_detail = []
    for i, col in col_vocab.items():
        decomp = col_decomp.get(col, {})
        order_info = col_order.get(col, {})
        col_vocab_detail.append({
            "col_index": i, "col_label": col,
            "process": decomp.get("process"),
            "occurrence": decomp.get("occurrence"),
            "step": decomp.get("step"),
            "variable": decomp.get("variable"),
            "rank_temporal": order_info.get("rank"),
            "median_timestamp": order_info.get("median_timestamp"),
            "conflict": order_info.get("conflict"),
        })

    # Row vocabulary enriched with design_version.
    # Primary source: df_long (always present in df_input.parquet).
    # Fallback: slopes_idx (only if df_long lacks the column).
    slopes_idx = df_slopes.set_index(trans_group_id)
    dv_available = trans_design_version_label in slopes_idx.columns

    dv_from_long = None
    if trans_design_version_label in df_long.columns:
        dv_from_long = (
            df_long.groupby(trans_group_id)[trans_design_version_label].first()
        )

    row_vocab_rows = []
    for k, v in row_vocab.items():
        entry: Dict[str, Any] = {"row_index": k, "group_id": v}
        if dv_from_long is not None and v in dv_from_long.index:
            entry[trans_design_version_label] = dv_from_long.loc[v]
        elif dv_available and v in slopes_idx.index:
            entry[trans_design_version_label] = slopes_idx.loc[v, trans_design_version_label]
        row_vocab_rows.append(entry)

    # ------------------------------------------------------------------
    # Build target DataFrame
    # ------------------------------------------------------------------
    target_series = _compute_target_series(df_slopes, target_sense)
    target_aligned = target_series.reindex(value_matrix.index)
    target_df = pd.DataFrame({
        trans_group_id: value_matrix.index.tolist(),
        _COL_SLOPE_A: slopes_idx.reindex(value_matrix.index)[_COL_SLOPE_A].values,
        _COL_SLOPE_B: slopes_idx.reindex(value_matrix.index)[_COL_SLOPE_B].values,
        "target": target_aligned.values,
        "target_sense": target_sense,
    })
    if dv_available:
        target_df[trans_design_version_label] = (
            slopes_idx.reindex(value_matrix.index)[trans_design_version_label].values
        )

    # ------------------------------------------------------------------
    # Build output matrix: promote index → named column, prepend group_id
    # and design_version as leading identifier columns so that every output
    # file is self-contained (no unnamed index required).
    #
    # design_version is read from df_long (which is always a slice of
    # df_input.parquet and reliably carries the column) rather than from
    # slopes_summary.csv, which may be an older cached file without it.
    # ------------------------------------------------------------------
    value_matrix_out = value_matrix.reset_index()   # group_id becomes col 0

    if trans_design_version_label in df_long.columns:
        dv_per_group = (
            df_long.groupby(trans_group_id)[trans_design_version_label]
            .first()
        )
        value_matrix_out.insert(
            1,
            trans_design_version_label,
            value_matrix_out[trans_group_id].map(dv_per_group),
        )
        logger.info(
            f"[partition {partition_id}] "
            "group_id + design_version prepended to design_matrix"
        )
    elif dv_available:
        # Fallback: use slopes_summary.csv if df_long lacks the column
        dv_from_slopes = slopes_idx[trans_design_version_label]
        value_matrix_out.insert(
            1,
            trans_design_version_label,
            value_matrix_out[trans_group_id].map(dv_from_slopes),
        )
        logger.info(
            f"[partition {partition_id}] "
            "group_id + design_version (from slopes) prepended to design_matrix"
        )
    else:
        logger.info(
            f"[partition {partition_id}] "
            "group_id prepended to design_matrix (design_version not available)"
        )

    # ------------------------------------------------------------------
    # Save outputs
    # ------------------------------------------------------------------
    n_conflicts_col = sum(1 for v in col_order.values() if v.get("conflict"))
    n_conflicts_row = sum(1 for v in row_order.values() if v.get("ordering_conflict") is not None)

    if not dry_run:
        value_matrix_out.to_parquet(join(part_dir, "design_matrix.parquet"), index=False)
        value_matrix_out.to_csv(join(part_dir, "design_matrix.csv"), index=False)
        target_df.to_csv(join(part_dir, "target.csv"), index=False)
        _save_json(col_vocab, join(part_dir, "column_vocabulary.json"))
        _save_json(row_vocab, join(part_dir, "row_vocabulary.json"))
        _save_json(col_order, join(part_dir, "column_order.json"))
        _save_json(row_order, join(part_dir, "row_order.json"))
        pd.DataFrame(col_vocab_detail).to_csv(
            join(part_dir, "column_vocabulary.csv"), index=False
        )
        pd.DataFrame(row_vocab_rows).to_csv(
            join(part_dir, "row_vocabulary.csv"), index=False
        )
        logger.info(
            f"[partition {partition_id}] Saved design_matrix "
            f"({value_matrix.shape}), target, vocabularies"
        )

    # Collect design_versions present in this partition
    _dv_set = {
        r.get(trans_design_version_label)
        for r in row_vocab_rows
        if r.get(trans_design_version_label) is not None
    }
    dv_list = sorted(str(v) for v in _dv_set)

    # col_vocab_detail is already in temporal rank order (built from
    # value_matrix.columns after _order_columns_by_timestamp).
    part_summary: Dict[str, Any] = {
        "partition_id": partition_id,
        "partition_dir": part_dir,
        "n_groups": len(groups),
        "n_cols": len(col_vocab),
        "n_ordering_conflicts_col": n_conflicts_col,
        "n_ordering_conflicts_row": n_conflicts_row,
        "hsic_applied": use_hsic and hsic_info is not None,
        "hsic_params_retained": hsic_info.get("n_params_after") if hsic_info else None,
        "hsic_params_filtered": hsic_info.get("n_params_filtered") if hsic_info else None,
        "dry_run": dry_run,
        "design_versions": dv_list,
        "n_design_versions": len(dv_list),
        "ordered_parameters": col_vocab_detail,
    }
    return part_summary


# ============================================================================
# MAIN PUBLIC FUNCTION
# ============================================================================

def generate_tabular_dataset(
    dataset_id: str,
    target_sense: str = "A",
    param_class: Optional[str] = None,
    dry_run: bool = False,
    slopes_path: Optional[str] = None,
    use_hsic: bool = False,
    hsic_target_sense: str = "A",
    hsic_threshold_multiplier: float = 1.0,
    discovery_only: bool = False,
    consolidation_coverage: Optional[float] = None,
    min_partition_samples: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Generate partitioned tabular design matrices from processed input data.

    Parameters
    ----------
    dataset_id : str
        Dataset identifier (folder name inside ``data/builds/``).
    target_sense : str
        Target response: ``'A'``, ``'B'``, or ``'max'``.
    param_class : str, optional
        Restrict to ``'read'`` (class=1), ``'set'`` (class=2), or
        ``None`` / ``'all'`` (default — keep all).
    dry_run : bool
        Log what *would* happen without saving any files.
    slopes_path : str, optional
        Custom path to ``slopes_summary.csv``.
    use_hsic : bool
        Apply HSIC-based parameter filtering per partition.  Default False.
    hsic_target_sense : str
        Sense used for HSIC (default ``'A'``).
    hsic_threshold_multiplier : float
        HSIC threshold multiplier k.  Default 1.0.

    Returns
    -------
    dict
        Top-level summary with keys:
        ``n_partitions``, ``partition_summaries``, ``output_dir``,
        ``elapsed_seconds``.

    discovery_only : bool
        If ``True``, only Stage 1-4 run: pivot, partition, save
        ``partition_overview.csv`` + ``partition_assignments.csv`` and return
        immediately.  No sub-folders or normalization are performed.
        Default ``False``.
    consolidation_coverage : float, optional
        Coverage threshold C ∈ (0, 1].  If provided, compatible partitions
        (those whose column intersection is ≥ C × max(|A|, |B|)) are merged
        via community detection + median imputation before Stage 5.
        ``None`` (default) disables consolidation.
    min_partition_samples : int, optional
        Partitions with fewer than this many groups are silently dropped
        before any files are written.  ``None`` (default) keeps all
        partitions.  Typical value: ``10``.

    Outputs (inside ``output/tabular_dataset/``)
    -------------------------------------------
    ``partition_overview.csv``
        One row per partition: ``partition_id``, ``n_groups``, ``n_cols``,
        ``design_versions`` (semicolon-separated), ``ordered_parameters``
        (semicolon-separated col_labels in temporal rank order).
    ``partition_assignments.csv``
        One row per group: ``group_id``, ``partition_id``.  Written in both
        discovery and full modes.
    ``consolidation_log.json``  *(only when consolidation_coverage is set)*
        Traceability: which source partitions merged into each consolidated
        partition and how many cells were imputed.
    ``partition_{n_rows}rows_{n_cols}cols_{hash8}/``
        Per-partition sub-folder with: design_matrix.parquet/csv,
        target.csv, column_vocabulary.json/csv, row_vocabulary.json/csv,
        column_order.json, row_order.json, denorm_map.json,
        [hsic_filter_info.json], partition_build.log.
    """
    if param_class not in _VALID_PARAM_CLASSES:
        raise ValueError(
            f"param_class must be one of {sorted(str(v) for v in _VALID_PARAM_CLASSES)}. "
            f"Got: {param_class!r}"
        )

    ROOT_DIR = get_root_dir()
    _, OUTPUT_DIR, _ = get_dirs(ROOT_DIR, dataset_id)
    tabular_dir = join(OUTPUT_DIR, TABULAR_OUTPUT_SUBDIR)
    if not exists(tabular_dir):
        makedirs(tabular_dir)

    fh = logging.FileHandler(join(tabular_dir, "tabular_build.log"), mode="a", encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(fh)
    logger.setLevel(logging.INFO)

    try:
        return _run_tabular(
            dataset_id=dataset_id,
            output_dir=OUTPUT_DIR,
            tabular_dir=tabular_dir,
            target_sense=target_sense,
            param_class=param_class,
            dry_run=dry_run,
            slopes_path=slopes_path,
            use_hsic=use_hsic,
            hsic_target_sense=hsic_target_sense,
            hsic_threshold_multiplier=hsic_threshold_multiplier,
            discovery_only=discovery_only,
            consolidation_coverage=consolidation_coverage,
            min_partition_samples=min_partition_samples,
        )
    finally:
        logger.removeHandler(fh)
        fh.close()


# ============================================================================
# IMPLEMENTATION
# ============================================================================

def _run_tabular(
    dataset_id: str,
    output_dir: str,
    tabular_dir: str,
    target_sense: str,
    param_class: Optional[str],
    dry_run: bool,
    slopes_path: Optional[str],
    use_hsic: bool,
    hsic_target_sense: str,
    hsic_threshold_multiplier: float,
    discovery_only: bool = False,
    consolidation_coverage: Optional[float] = None,
    min_partition_samples: Optional[int] = None,
) -> Dict[str, Any]:

    ts_start = datetime.now()
    logger.info("=" * 72)
    logger.info(f"Tabular dataset generation started: {ts_start.isoformat()}")
    logger.info(f"  dataset_id   : {dataset_id}")
    logger.info(f"  target_sense : {target_sense}")
    logger.info(f"  param_class  : {param_class!r}")
    logger.info(f"  use_hsic     : {use_hsic}")
    logger.info(f"  dry_run      : {dry_run}")
    logger.info("=" * 72)

    # =====================================================================
    # STAGE 1 – LOAD
    # =====================================================================
    df_input_path = join(output_dir, trans_df_input)
    if not exists(df_input_path):
        raise FileNotFoundError(
            f"df_input.parquet not found: {df_input_path}\n"
            "Run process_raw() + filter_sparse_columns() first."
        )

    df_input = pd.read_parquet(df_input_path)
    logger.info(
        f"[Stage 1] df_input loaded: {len(df_input):,} rows, "
        f"{df_input[trans_group_id].nunique():,} groups"
    )

    df_slopes = _load_slopes(output_dir, slopes_path)
    groups_in_slopes = set(df_slopes[trans_group_id].unique())
    groups_in_input  = set(df_input[trans_group_id].unique())
    common_groups = groups_in_input & groups_in_slopes
    logger.info(
        f"  Common groups (input ^ slopes): {len(common_groups)} "
        f"(input-only: {len(groups_in_input - groups_in_slopes)}, "
        f"slope-only: {len(groups_in_slopes - groups_in_input)})"
    )
    if not common_groups:
        raise ValueError("No groups in common between df_input and slopes_summary.")

    df_input = df_input[df_input[trans_group_id].isin(common_groups)].copy()

    # =====================================================================
    # STAGE 2 – PARAM CLASS FILTER
    # =====================================================================
    if param_class in _PARAM_CLASS_MAP:
        class_val = _PARAM_CLASS_MAP[param_class]
        if trans_class_label not in df_input.columns:
            raise ValueError(
                f"param_class='{param_class}' requested but '{trans_class_label}' "
                "column absent in df_input.parquet."
            )
        n_before = len(df_input)
        df_input = df_input[df_input[trans_class_label] == class_val].copy()
        logger.info(
            f"[Stage 2] param_class='{param_class}': "
            f"{n_before:,} -> {len(df_input):,} rows"
        )
    else:
        logger.info("[Stage 2] param_class filter: none")

    # Ensure datetime type
    has_date = trans_date_label in df_input.columns
    if has_date:
        df_input[trans_date_label] = pd.to_datetime(df_input[trans_date_label], errors="coerce")

    # =====================================================================
    # STAGE 3 – PIVOT TO WIDE (raw values)
    # =====================================================================
    logger.info("[Stage 3] Pivoting to wide design matrix (raw values)...")
    col_labels, col_decomp = _build_col_labels(df_input)
    df_input = df_input.copy()
    df_input["_col_label"] = col_labels

    value_matrix: pd.DataFrame = (
        df_input
        .groupby([trans_group_id, "_col_label"])[trans_value_label]
        .mean()
        .unstack()
    )
    value_matrix.columns.name = None

    if has_date:
        ts_matrix_all: pd.DataFrame = (
            df_input
            .groupby([trans_group_id, "_col_label"])[trans_date_label]
            .first()
            .unstack()
        )
        ts_matrix_all.columns.name = None
        for col in ts_matrix_all.columns:
            ts_matrix_all[col] = pd.to_datetime(ts_matrix_all[col], errors="coerce")
    else:
        ts_matrix_all = pd.DataFrame(
            index=value_matrix.index, columns=value_matrix.columns,
            dtype="datetime64[ns]",
        )

    logger.info(f"  Wide matrix: {value_matrix.shape}")

    # =====================================================================
    # STAGE 4 – PARTITION BY EXACT NON-NULL COLUMN PATTERN
    # =====================================================================
    logger.info("[Stage 4] Partitioning by exact non-null column pattern...")
    partitions = _partition_by_column_pattern(value_matrix)
    # Enrich partition IDs with row count → "{n_rows}rows_{n_cols}cols_{hash8}"
    partitions = {
        f"{len(grps)}rows_{pid}": grps
        for pid, grps in partitions.items()
    }
    logger.info(f"  {len(partitions)} partition(s) found")
    for pid, grps in sorted(partitions.items(), key=lambda x: -len(x[1])):
        logger.info(f"    partition_{pid}: {len(grps)} groups")

    # =====================================================================
    # STAGE 4.5 – ASSIGNMENTS / DISCOVERY SHORTCUT / CONSOLIDATION
    # =====================================================================

    # When no consolidation will run, raw partitions ARE the final state.
    # Apply min_partition_samples filter here, before any output is written.
    if min_partition_samples is not None and min_partition_samples > 0 and consolidation_coverage is None:
        n_before_f = len(partitions)
        small_raw = {
            pid for pid, grps in partitions.items()
            if len(grps) < min_partition_samples
        }
        if small_raw:
            for pid in sorted(small_raw, key=lambda p: len(partitions[p])):
                logger.info(
                    f"[Stage 4.5] Skipping partition {pid}: "
                    f"{len(partitions[pid])} group(s) < "
                    f"min_partition_samples={min_partition_samples}"
                )
            partitions = {
                pid: grps for pid, grps in partitions.items()
                if pid not in small_raw
            }
            logger.info(
                f"[Stage 4.5] Removed {len(small_raw)} small partition(s) "
                f"({n_before_f} -> {len(partitions)} remaining)"
            )

    # Always write partition_assignments.csv — used by consolidation + traceability
    assign_rows = [
        {"group_id": str(g), "partition_id": pid}
        for pid, grps in partitions.items()
        for g in grps
    ]
    pd.DataFrame(assign_rows).to_csv(
        join(tabular_dir, "partition_assignments.csv"), index=False
    )
    logger.info(
        f"[Stage 4.5] Saved partition_assignments.csv ({len(assign_rows)} groups)"
    )

    # Raw partition overview — always written here (fast, before any
    # processing or merging) so it is available regardless of discovery_only.
    logger.info("[Stage 4.5] Building raw partition overview (global timestamp order)...")
    global_rank = _global_column_order(ts_matrix_all)

    dv_from_input = None
    if trans_design_version_label in df_input.columns:
        dv_from_input = df_input.groupby(trans_group_id)[trans_design_version_label].first()

    raw_overview_rows: List[Dict] = []
    for pid, grps in sorted(partitions.items(), key=lambda x: -len(x[1])):
        sample_group = grps[0]
        part_cols = sorted(
            [c for c in value_matrix.columns if pd.notna(value_matrix.loc[sample_group, c])],
            key=lambda c: (global_rank.get(c, 999_999), c),
        )
        if dv_from_input is not None:
            dv_set = {
                str(dv_from_input.loc[g])
                for g in grps
                if g in dv_from_input.index and pd.notna(dv_from_input.loc[g])
            }
            dv_str = "; ".join(sorted(dv_set))
        else:
            dv_str = ""
        raw_overview_rows.append({
            "partition_id":       pid,
            "n_groups":           len(grps),
            "n_cols":             len(part_cols),
            "design_versions":    dv_str,
            "ordered_parameters": "; ".join(part_cols),
        })

    pd.DataFrame(raw_overview_rows).to_csv(
        join(tabular_dir, "partition_overview.csv"), index=False
    )
    logger.info(
        f"[Stage 4.5] Saved partition_overview.csv ({len(raw_overview_rows)} raw partition(s))"
    )

    # Expected merge preview — always written when consolidation_coverage is set.
    if consolidation_coverage is not None:
        col_sets_preview = {
            pid: frozenset(
                c for c in value_matrix.columns
                if pd.notna(value_matrix.loc[grps[0], c])
            )
            for pid, grps in partitions.items()
        }
        preview = _preview_consolidation(
            partitions=partitions,
            col_sets=col_sets_preview,
            coverage_threshold=consolidation_coverage,
        )
        preview_rows = [
            {
                "merged_partition_id":  r["merged_partition_id"],
                "n_groups":             r["n_groups"],
                "n_cols":               r["n_cols"],
                "n_source_partitions":  r["n_source_partitions"],
                "source_partitions":    "; ".join(r["source_partitions"]),
            }
            for r in preview
        ]
        pd.DataFrame(preview_rows).to_csv(
            join(tabular_dir, "expected_merged_partitions.csv"), index=False
        )
        logger.info(
            f"[Stage 4.5] Saved expected_merged_partitions.csv — "
            f"{len(preview)} merged partition(s) from {len(partitions)} raw "
            f"(C={consolidation_coverage:.3f})"
        )

    # Early exit for discovery-only mode.
    if discovery_only:
        logger.info("[Stage 4.5] discovery_only=True — stopping before processing")
        ts_end = datetime.now()
        return {
            "mode":            "discovery",
            "dataset_id":      dataset_id,
            "n_partitions":    len(partitions),
            "output_dir":      tabular_dir,
            "elapsed_seconds": round((ts_end - ts_start).total_seconds(), 2),
        }

    # Optional consolidation (Phase 2+3 path)
    prebuilt_matrices: Optional[Dict[str, pd.DataFrame]] = None
    merge_log: Optional[Dict[str, Any]] = None          # populated if consolidation runs
    if consolidation_coverage is not None:
        logger.info(
            f"[Stage 4.5] Consolidating partitions "
            f"(coverage_threshold={consolidation_coverage:.3f}) ..."
        )
        col_sets = {
            pid: frozenset(
                c for c in value_matrix.columns
                if pd.notna(value_matrix.loc[grps[0], c])
            )
            for pid, grps in partitions.items()
        }
        partitions, vm_consolidated, merge_log = _consolidate_partitions(  # type: ignore[assignment]
            partitions=partitions,
            col_sets=col_sets,
            value_matrix=value_matrix,
            coverage_threshold=consolidation_coverage,
        )
        logger.info(
            f"[Stage 4.5] Consolidation: "
            f"{len(col_sets)} -> {len(partitions)} partition(s)"
        )
        _save_json(merge_log, join(tabular_dir, "consolidation_log.json"))
        logger.info("[Stage 4.5] Saved consolidation_log.json")

        # Filter small merged partitions before writing the overview or processing.
        # Small raw partitions may have merged into large enough combined partitions —
        # that is why the filter is applied here (post-merge) rather than pre-merge.
        if min_partition_samples is not None and min_partition_samples > 0:
            n_before_m = len(partitions)
            small_merged = {
                pid for pid, grps in partitions.items()
                if len(grps) < min_partition_samples
            }
            if small_merged:
                for pid in sorted(small_merged, key=lambda p: len(partitions[p])):
                    logger.info(
                        f"[Stage 4.5] Skipping merged partition {pid}: "
                        f"{len(partitions[pid])} group(s) < "
                        f"min_partition_samples={min_partition_samples}"
                    )
                partitions = {
                    pid: grps for pid, grps in partitions.items()
                    if pid not in small_merged
                }
                logger.info(
                    f"[Stage 4.5] Removed {len(small_merged)} small merged partition(s) "
                    f"({n_before_m} -> {len(partitions)} remaining)"
                )

        # Write merged_partition_overview.csv here (Stage 4.5, before Stage 5)
        # so it is guaranteed to exist even if Stage 5 fails.
        merged_overview_rows: List[Dict] = []
        for pid, grps in sorted(partitions.items(), key=lambda x: -len(x[1])):
            sample_grp = grps[0]
            m_cols = sorted(
                [c for c in vm_consolidated.columns if pd.notna(vm_consolidated.loc[sample_grp, c])],
                key=lambda c: (global_rank.get(c, 999_999), c),
            )
            log_entry = merge_log.get(pid, {})
            source_pids = log_entry.get("source_partitions", [pid])
            if dv_from_input is not None:
                dv_set = {
                    str(dv_from_input.loc[g])
                    for g in grps
                    if g in dv_from_input.index and pd.notna(dv_from_input.loc[g])
                }
                dv_str = "; ".join(sorted(dv_set))
            else:
                dv_str = ""
            merged_overview_rows.append({
                "merged_partition_id":  pid,
                "n_groups":             len(grps),
                "n_cols":               len(m_cols),
                "n_source_partitions":  len(source_pids),
                "source_partitions":    "; ".join(source_pids),
                "design_versions":      dv_str,
                "ordered_parameters":   "; ".join(m_cols),
            })
        pd.DataFrame(merged_overview_rows).to_csv(
            join(tabular_dir, "merged_partition_overview.csv"), index=False
        )
        logger.info(
            f"[Stage 4.5] Saved merged_partition_overview.csv "
            f"({len(merged_overview_rows)} merged partition(s))"
        )

        # Slice the imputed matrix per consolidated partition so that
        # _process_partition_inner can use pre-imputed values directly
        prebuilt_matrices = {}
        for pid, grps in partitions.items():
            sample = grps[0]
            part_cols = [
                c for c in vm_consolidated.columns
                if pd.notna(vm_consolidated.loc[sample, c])
            ]
            prebuilt_matrices[pid] = vm_consolidated.loc[grps, part_cols].copy()

    # =====================================================================
    # STAGE 5 – PER-PARTITION PROCESSING
    # =====================================================================
    logger.info("[Stage 5] Processing partitions...")
    partition_summaries: List[Dict] = []
    for pid, grps in sorted(partitions.items(), key=lambda x: -len(x[1])):
        logger.info(f"  --- partition_{pid} ({len(grps)} groups) ---")
        part_summary = _process_partition(
            partition_id=pid,
            groups=grps,
            df_long_all=df_input,
            df_slopes=df_slopes,
            col_decomp=col_decomp,
            ts_matrix_all=ts_matrix_all,
            value_matrix_all=value_matrix,
            target_sense=target_sense,
            tabular_dir=tabular_dir,
            use_hsic=use_hsic,
            hsic_target_sense=hsic_target_sense,
            hsic_threshold_multiplier=hsic_threshold_multiplier,
            dry_run=dry_run,
            prebuilt_value_matrix=(
                prebuilt_matrices[pid] if prebuilt_matrices else None
            ),
        )
        partition_summaries.append(part_summary)

    # =====================================================================
    # STAGE 6 – SAVE MERGED PARTITION OVERVIEW
    # =====================================================================
    # partition_overview.csv was already written in Stage 4.5 (raw partitions).
    # Here we write merged_partition_overview.csv — only when consolidation ran.
    # merged_partition_overview.csv is written in Stage 4.5 (after consolidation)
    # to guarantee it exists even if Stage 5 later fails.  Nothing to do here.

    # =====================================================================
    # SUMMARY (returned to caller; not saved to disk)
    # =====================================================================
    ts_end = datetime.now()
    summary: Dict[str, Any] = {
        "dataset_id":          dataset_id,
        "target_sense":        target_sense,
        "param_class_filter":  param_class if param_class is not None else "all",
        "use_hsic":            use_hsic,
        "dry_run":             dry_run,
        "timestamp":           ts_end.isoformat(),
        "n_partitions":        len(partition_summaries),
        "partition_summaries": partition_summaries,
        "output_dir":          tabular_dir,
        "elapsed_seconds":     round((ts_end - ts_start).total_seconds(), 2),
    }

    logger.info("=" * 72)
    logger.info(
        f"DONE | {len(partition_summaries)} partition(s) | "
        f"elapsed {summary['elapsed_seconds']}s"
    )
    logger.info("=" * 72)
    return summary


# ============================================================================
# STANDALONE ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Generate partitioned tabular design matrices."
    )
    parser.add_argument("dataset_id")
    parser.add_argument("--target_sense", default="A", choices=["A", "B", "max"])
    parser.add_argument("--param_class", default=None, choices=["read", "set", "all"])
    parser.add_argument("--dry_run", action="store_true")
    parser.add_argument("--use_hsic", action="store_true")
    parser.add_argument("--hsic_sense", default="A")
    parser.add_argument("--hsic_k", type=float, default=1.0)
    args = parser.parse_args()

    result = generate_tabular_dataset(
        dataset_id=args.dataset_id,
        target_sense=args.target_sense,
        param_class=args.param_class,
        dry_run=args.dry_run,
        use_hsic=args.use_hsic,
        hsic_target_sense=args.hsic_sense,
        hsic_threshold_multiplier=args.hsic_k,
    )
    print(f"\nDone: {result['n_partitions']} partition(s) in {result['elapsed_seconds']}s")
    for ps in result["partition_summaries"]:
        print(f"  partition_{ps['partition_id']}: "
              f"{ps['n_groups']} groups × {ps['n_cols']} cols")
