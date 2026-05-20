"""
Backbone IST slope computation, classification, and normalization.

This module processes IST (In-System Test) time-series data to produce
``slopes_summary.csv`` — a dataset-level file that is independent of HSIC
and serves as the canonical target source for both the sequence and tabular
dataset branches.

Pipeline
--------
1. Load ``df_trg.csv`` from the control folder.
2. Compute per-sample regression slopes via ``data_analysis.hsic.compute_slopes``.
3. Pivot to wide format via ``data_analysis.hsic.pivot_slopes``.
4. **Classify** each sample (on raw slopes): ``ist_class = 1 (FAIL)``
   if ``slope > fail_threshold`` (default 0.005 = 1/200 cycle, i.e. 10%
   resistance increase over 200 cycles in the normalized resistance space).
5. **Normalize** slope values for ML use (applied *after* classification).
6. Save ``output/slopes_summary.csv`` (backbone output).

The file written here is the authoritative source for:
- ``generate_tabular_dataset`` (target column)
- ``filter_by_hsic`` (HSIC target; a copy is placed in ``output/hsic_analysis/``
  so the existing HSIC caching mechanism finds it transparently)

Column schema of ``slopes_summary.csv``
----------------------------------------
group               original IST group identifier
slope_{var}_raw     raw regression slope (physical units)
r2_{var}            R² of the linear fit
ist_class_{var}     1 = FAIL  (slope > fail_threshold), 0 = PASS
ist_class_max       1 = FAIL if any sense fails
slope_{var}_norm    min-max (or z-score) normalized slope for ML
"""

import logging
import shutil
from datetime import datetime
from os import makedirs
from os.path import abspath, dirname, exists, join
from typing import Any, Dict, Optional

import pandas as pd

import sys
ROOT_DIR = dirname(dirname(dirname(abspath(__file__))))
sys.path.append(ROOT_DIR)

from proT_pipeline.labels import (
    get_dirs,
    get_root_dir,
    target_filename,
    target_sep,
    trans_design_version_label,
    trans_group_id,
    trans_variable_label,
)
from proT_pipeline.utils import safe_read_csv
from data_analysis.hsic import compute_slopes, pivot_slopes

# ============================================================================
# CONSTANTS
# ============================================================================

SLOPES_FILENAME = "slopes_summary.csv"
HSIC_SUBDIR = "hsic_analysis"

# Default IST FAIL criterion:
# slope > 1/200 ≈ 0.005  in normalized resistance/cycle units
# (resistance normalized so that 1 = 10% change; 10% / 200 cycles = 0.005)
DEFAULT_FAIL_THRESHOLD = 1.0 / 200.0

logger = logging.getLogger(__name__)


# ============================================================================
# NORMALIZATION HELPERS
# ============================================================================

def _minmax_normalize(series: pd.Series) -> pd.Series:
    """Min-max normalize a Series to [0, 1]. Returns zeros if range is zero."""
    lo, hi = series.min(), series.max()
    if hi == lo:
        return pd.Series(0.0, index=series.index, name=series.name)
    return (series - lo) / (hi - lo)


def _zscore_normalize(series: pd.Series) -> pd.Series:
    """Z-score normalize a Series. Returns zeros if std is zero."""
    mu, sigma = series.mean(), series.std()
    if sigma == 0:
        return pd.Series(0.0, index=series.index, name=series.name)
    return (series - mu) / sigma


def _normalize(series: pd.Series, method: str) -> pd.Series:
    if method == "minmax":
        return _minmax_normalize(series)
    elif method == "zscore":
        return _zscore_normalize(series)
    else:
        raise ValueError(f"norm_method must be 'minmax' or 'zscore'. Got: {method!r}")


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def compute_ist_slopes(
    dataset_id: str,
    fail_threshold: float = DEFAULT_FAIL_THRESHOLD,
    norm_method: str = "minmax",
    force_recompute: bool = False,
) -> Dict[str, Any]:
    """
    Compute IST slopes, classify pass/fail, and normalize for ML targets.

    Parameters
    ----------
    dataset_id : str
        Dataset identifier (folder name inside ``data/builds/``).
    fail_threshold : float
        Raw slope value above which a sample is labelled FAIL (class = 1).
        Default 1/200 ≈ 0.005 (10% resistance increase over 200 cycles in
        normalized resistance/cycle units).
    norm_method : str
        Normalization applied to slope values AFTER classification.
        ``'minmax'`` (default) → scale to [0, 1].
        ``'zscore'`` → zero-mean, unit-variance.
    force_recompute : bool
        If ``True``, recompute slopes even if ``slopes_summary.csv`` already
        exists.  Default ``False`` (use cached file if present).

    Returns
    -------
    dict
        Summary with keys: ``n_samples``, ``n_fail_A``, ``n_fail_B``,
        ``n_fail_max``, ``fail_threshold``, ``norm_method``, ``output_path``.

    Outputs
    -------
    ``output/slopes_summary.csv``
        Canonical backbone slopes file.  Also copied to
        ``output/hsic_analysis/slopes_summary.csv`` so that the HSIC
        analysis caching logic finds it without recomputing slopes.
    """
    ROOT_DIR = get_root_dir()
    INPUT_DIR, OUTPUT_DIR, CONTROL_DIR = get_dirs(ROOT_DIR, dataset_id)

    slopes_path = join(OUTPUT_DIR, SLOPES_FILENAME)
    hsic_dir = join(OUTPUT_DIR, HSIC_SUBDIR)
    hsic_slopes_path = join(hsic_dir, SLOPES_FILENAME)

    # ------------------------------------------------------------------
    # Caching check
    # ------------------------------------------------------------------
    if exists(slopes_path) and not force_recompute:
        logger.info(
            f"[compute_ist_slopes] Found cached {slopes_path}. "
            "Skipping recomputation (set force_recompute=True to override)."
        )
        df = pd.read_csv(slopes_path)
        _ensure_hsic_copy(slopes_path, hsic_slopes_path, hsic_dir, logger)
        return _build_summary(df, fail_threshold, norm_method, slopes_path)

    # ------------------------------------------------------------------
    # Load target data
    # ------------------------------------------------------------------
    df_trg_path = join(CONTROL_DIR, target_filename)
    if not exists(df_trg_path):
        raise FileNotFoundError(
            f"Target file not found: {df_trg_path}\n"
            "Run the target processing pipeline first to create df_trg.csv."
        )
    df_trg = safe_read_csv(df_trg_path, sep=target_sep)
    logger.info(
        f"[compute_ist_slopes] Loaded df_trg: {len(df_trg):,} rows, "
        f"{df_trg[trans_group_id].nunique():,} unique groups"
    )

    # ------------------------------------------------------------------
    # Stage 1 – Compute regression slopes
    # ------------------------------------------------------------------
    logger.info("[compute_ist_slopes] Computing regression slopes...")
    slopes_long = compute_slopes(df_trg)
    slopes_wide = pivot_slopes(slopes_long)
    # Original pivot column names (e.g. slope_delta_A_norm) are kept as-is for
    # backward-compat.  We also store a _raw copy BEFORE normalization.
    orig_slope_cols = [c for c in slopes_wide.columns if c.startswith("slope_")]
    raw_col_map: Dict[str, str] = {}   # orig_col -> raw_col
    for orig_col in orig_slope_cols:
        raw_col = orig_col + "_raw"
        slopes_wide[raw_col] = slopes_wide[orig_col]
        raw_col_map[orig_col] = raw_col

    logger.info(
        f"[compute_ist_slopes] Slopes computed: {len(slopes_wide):,} samples, "
        f"original slope columns: {orig_slope_cols}"
    )

    # ------------------------------------------------------------------
    # Stage 1.5 – Append design_version metadata from df_trg
    # design_version (e.g. "453828_A1") is constant per group and provides
    # traceability context in slopes_summary.csv and all downstream outputs.
    # ------------------------------------------------------------------
    if trans_design_version_label in df_trg.columns:
        meta = (
            df_trg.groupby(trans_group_id)[trans_design_version_label]
            .first()   # constant within group
            .reset_index()
        )
        slopes_wide = slopes_wide.merge(meta, on=trans_group_id, how="left")
        logger.info(
            f"[compute_ist_slopes] design_version metadata appended "
            f"({slopes_wide[trans_design_version_label].nunique()} unique values)"
        )
    else:
        logger.info(
            f"[compute_ist_slopes] '{trans_design_version_label}' not found in "
            "df_trg — slopes_summary.csv will not include design_version."
        )

    # ------------------------------------------------------------------
    # Stage 2 – IST classification (on raw slopes, BEFORE normalization)
    # ------------------------------------------------------------------
    import re as _re

    logger.info(
        f"[compute_ist_slopes] Classifying samples "
        f"(fail_threshold={fail_threshold:.6f})..."
    )

    for orig_col, raw_col in raw_col_map.items():
        # Extract single uppercase sense letter from the variable tag.
        # e.g. "slope_delta_A_norm" -> variable tag "delta_A_norm" -> sense "A"
        var_tag = orig_col[len("slope_"):]   # e.g. "delta_A_norm"
        m = _re.search(r"_([A-Z])(?:_|$)", var_tag)
        sense_letter = m.group(1) if m else var_tag   # fallback: full tag
        class_col = f"ist_class_{sense_letter}"

        slopes_wide[class_col] = (slopes_wide[raw_col] > fail_threshold).astype(int)
        n_fail = int(slopes_wide[class_col].sum())
        n_total = len(slopes_wide)
        logger.info(
            f"  {class_col} (from {raw_col}): {n_fail}/{n_total} FAIL "
            f"({100 * n_fail / n_total:.1f}%)"
        )

    # Composite: FAIL if ANY per-sense column fires
    class_cols = [c for c in slopes_wide.columns if c.startswith("ist_class_")]
    slopes_wide["ist_class_max"] = slopes_wide[class_cols].max(axis=1).astype(int)
    n_fail_max = int(slopes_wide["ist_class_max"].sum())
    logger.info(
        f"  ist_class_max (any sense): {n_fail_max}/{len(slopes_wide)} FAIL"
    )

    # ------------------------------------------------------------------
    # Stage 3 – Normalize slopes for ML (applied AFTER classification)
    # The normalized values overwrite the original pivot column names
    # (e.g. slope_delta_A_norm) so that downstream consumers find the
    # expected column names.  Raw values remain in *_raw columns.
    # ------------------------------------------------------------------
    logger.info(
        f"[compute_ist_slopes] Normalizing slopes (method='{norm_method}')..."
    )
    for orig_col, raw_col in raw_col_map.items():
        normalized = _normalize(
            slopes_wide[raw_col].dropna().reindex(slopes_wide.index),
            norm_method,
        )
        slopes_wide[orig_col] = normalized   # overwrite in-place (backward compat)
        logger.info(
            f"  {orig_col} (normalized): "
            f"min={slopes_wide[orig_col].min():.4f}  "
            f"max={slopes_wide[orig_col].max():.4f}  "
            f"mean={slopes_wide[orig_col].mean():.4f}"
        )

    # ------------------------------------------------------------------
    # Stage 4 – Save
    # ------------------------------------------------------------------
    slopes_wide.to_csv(slopes_path, index=False)
    logger.info(f"[compute_ist_slopes] Saved {slopes_path}")

    # Copy to hsic_analysis/ so HSIC caching mechanism finds it
    _ensure_hsic_copy(slopes_path, hsic_slopes_path, hsic_dir, logger)

    return _build_summary(slopes_wide, fail_threshold, norm_method, slopes_path)


# ============================================================================
# PRIVATE HELPERS
# ============================================================================

def _ensure_hsic_copy(
    src: str, dst: str, hsic_dir: str, logger_: logging.Logger
) -> None:
    """Copy backbone slopes to output/hsic_analysis/ only if that folder already exists.

    In a tabular-only run the folder is never created, so no misleading
    ``hsic_analysis/`` directory appears outside the per-partition outputs.
    In a sequential run ``filter_by_hsic()`` / ``run_hsic_analysis()`` creates
    the folder before it is needed; subsequent calls to ``compute_ist_slopes``
    then keep the copy up-to-date.
    """
    if not exists(hsic_dir):
        return  # Don't create the folder; avoids a stray hsic_analysis/ dir
                # in tabular-only pipelines.
    if not exists(dst):
        shutil.copy2(src, dst)
        logger_.info(
            f"[compute_ist_slopes] Copied slopes to HSIC cache: {dst}"
        )
    else:
        logger_.debug(
            f"[compute_ist_slopes] HSIC cache already present, not overwriting: {dst}"
        )


def _build_summary(
    df: pd.DataFrame,
    fail_threshold: float,
    norm_method: str,
    output_path: str,
) -> Dict[str, Any]:
    """Build and return the summary dict from a (possibly cached) slopes DataFrame."""
    summary: Dict[str, Any] = {
        "n_samples": len(df),
        "fail_threshold": fail_threshold,
        "norm_method": norm_method,
        "output_path": output_path,
    }
    for col in df.columns:
        if col.startswith("ist_class_"):
            summary[f"n_fail_{col[len('ist_class_'):]}"] = int(df[col].sum())
    return summary
