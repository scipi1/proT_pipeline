"""
Partition consolidation for the proT tabular dataset pipeline.

Two partitions A and B are *compatible* when their column intersection is
large enough relative to the larger of the two:

    |A ∩ B| >= C * max(|A|, |B|)          C ∈ (0, 1]

All partitions in the same connected component of the compatibility graph
are merged into one consolidated partition:

* **Columns**  = union of all column sets in the component.
* **Rows**     = union of all group IDs in the component.
* **Imputation**: for every column that is naturally missing in a row (i.e.
  the row came from a partition that did not have that column), fill with the
  per-column median computed only from rows that *naturally* carry that column
  (no imputed values are used to compute the median).

The merge log records which source partitions contributed to each consolidated
partition and how many cells were imputed, enabling full traceability.

Typical usage (Phase 2 of the two-phase workflow)
-------------------------------------------------
::

    from proT_pipeline.input_processing.consolidate_partitions import consolidate

    new_partitions, new_value_matrix, merge_log = consolidate(
        partitions=partitions,           # {pid: [group_id, ...]}
        col_sets=col_sets,               # {pid: frozenset of col labels}
        value_matrix=value_matrix,       # pd.DataFrame indexed by group_id
        coverage_threshold=0.9,
    )
"""

import hashlib
import logging
from collections import defaultdict
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ===========================================================================
# PUBLIC API
# ===========================================================================

def preview_consolidation(
    partitions: Dict[str, List],
    col_sets: Dict[str, FrozenSet[str]],
    coverage_threshold: float,
) -> List[Dict[str, Any]]:
    """
    Preview the result of consolidation **without** imputing any values.

    Runs only the community-detection step (Union-Find) and returns a list of
    merged-partition descriptors — one per connected component.  This is fast
    even for large numbers of partitions and is designed to be called during
    Phase 1 (``discovery_only=True``) so the user can inspect expected merges
    before committing to Phase 2+3.

    Parameters
    ----------
    partitions : dict
        ``{partition_id: [group_id, ...]}``
    col_sets : dict
        ``{partition_id: frozenset of column labels}``
    coverage_threshold : float
        C ∈ (0, 1].  Two partitions are compatible when
        ``|A ∩ B| >= C * max(|A|, |B|)``.

    Returns
    -------
    list of dict
        Each entry represents one merged (or singleton) partition:
        ``{"merged_partition_id": str, "n_groups": int, "n_cols": int,
           "n_source_partitions": int, "source_partitions": [str, ...]}``
        Sorted descending by ``n_groups``.
    """
    if not (0.0 < coverage_threshold <= 1.0):
        raise ValueError(
            f"coverage_threshold must be in (0, 1]. Got: {coverage_threshold}"
        )

    pids = list(partitions.keys())
    n = len(pids)
    if n == 0:
        return []

    # Union-Find
    parent = list(range(n))
    rank   = [0] * n

    def _find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def _union(x: int, y: int) -> None:
        rx, ry = _find(x), _find(y)
        if rx == ry:
            return
        if rank[rx] < rank[ry]:
            rx, ry = ry, rx
        parent[ry] = rx
        if rank[rx] == rank[ry]:
            rank[rx] += 1

    pid_sizes = {pid: len(col_sets[pid]) for pid in pids}
    for i in range(n):
        size_i = pid_sizes[pids[i]]
        cols_i = col_sets[pids[i]]
        for j in range(i + 1, n):
            size_j = pid_sizes[pids[j]]
            if min(size_i, size_j) < coverage_threshold * max(size_i, size_j):
                continue
            if len(cols_i & col_sets[pids[j]]) >= coverage_threshold * max(size_i, size_j):
                _union(i, j)

    # Connected components
    components: Dict[int, List[int]] = defaultdict(list)
    for i in range(n):
        components[_find(i)].append(i)

    results = []
    for member_idxs in components.values():
        source_pids = [pids[i] for i in member_idxs]
        all_groups: List = []
        for pid in source_pids:
            all_groups.extend(partitions[pid])
        union_cols: FrozenSet[str] = frozenset().union(
            *[col_sets[pid] for pid in source_pids]
        )
        merged_pid = (
            f"{len(all_groups)}rows_{len(union_cols)}cols_"
            + hashlib.md5(",".join(sorted(source_pids)).encode()).hexdigest()[:8]
        )
        results.append({
            "merged_partition_id":  merged_pid,
            "n_groups":             len(all_groups),
            "n_cols":               len(union_cols),
            "n_source_partitions":  len(source_pids),
            "source_partitions":    source_pids,
        })

    results.sort(key=lambda r: -r["n_groups"])
    return results


def consolidate(
    partitions: Dict[str, List],
    col_sets: Dict[str, FrozenSet[str]],
    value_matrix: pd.DataFrame,
    coverage_threshold: float,
) -> Tuple[Dict[str, List], pd.DataFrame, Dict[str, Any]]:
    """
    Merge compatible partitions via community detection + median imputation.

    Parameters
    ----------
    partitions : dict
        ``{partition_id: [group_id, ...]}``
    col_sets : dict
        ``{partition_id: frozenset of column labels}``
    value_matrix : pd.DataFrame
        Full wide matrix (raw, un-normalised values), indexed by group_id.
        Columns are parameter labels.
    coverage_threshold : float
        C ∈ (0, 1].  Two partitions A and B are compatible when
        ``|A ∩ B| >= C * max(|A|, |B|)``.

    Returns
    -------
    new_partitions : dict
        ``{merged_partition_id: [group_id, ...]}`` — one entry per
        connected component (merged partitions have a deterministic new ID).
    new_value_matrix : pd.DataFrame
        Copy of ``value_matrix`` extended with imputed values where needed.
        Only rows and columns involved in at least one merge are modified.
    merge_log : dict
        ``{merged_partition_id: {"source_partitions": [...],
                                  "n_imputed_cells": int,
                                  "n_groups": int,
                                  "n_cols": int}}``
    """
    if not (0.0 < coverage_threshold <= 1.0):
        raise ValueError(
            f"coverage_threshold must be in (0, 1]. Got: {coverage_threshold}"
        )

    pids = list(partitions.keys())
    n = len(pids)
    logger.info(
        f"[consolidate] {n} partitions | coverage_threshold={coverage_threshold:.3f}"
    )

    if n == 0:
        return {}, value_matrix.copy(), {}

    # -----------------------------------------------------------------------
    # 1. Union-Find initialisation
    # -----------------------------------------------------------------------
    parent = list(range(n))
    rank   = [0] * n

    def _find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]   # path compression
            x = parent[x]
        return x

    def _union(x: int, y: int) -> None:
        rx, ry = _find(x), _find(y)
        if rx == ry:
            return
        if rank[rx] < rank[ry]:
            rx, ry = ry, rx
        parent[ry] = rx
        if rank[rx] == rank[ry]:
            rank[rx] += 1

    # -----------------------------------------------------------------------
    # 2. Pairwise compatibility check — O(n²)
    #    Optimisation: skip pair (i,j) early when the maximum possible
    #    intersection (= min(|A|,|B|)) is already below the threshold.
    # -----------------------------------------------------------------------
    n_edges = 0
    pid_sizes = {pid: len(col_sets[pid]) for pid in pids}

    for i in range(n):
        pid_i = pids[i]
        cols_i = col_sets[pid_i]
        size_i = pid_sizes[pid_i]
        for j in range(i + 1, n):
            pid_j  = pids[j]
            size_j = pid_sizes[pid_j]
            # Early exit: even perfect overlap cannot meet threshold
            if min(size_i, size_j) < coverage_threshold * max(size_i, size_j):
                continue
            intersection_size = len(cols_i & col_sets[pid_j])
            if intersection_size >= coverage_threshold * max(size_i, size_j):
                _union(i, j)
                n_edges += 1

    logger.info(f"[consolidate] {n_edges} compatible pair(s) found")

    # -----------------------------------------------------------------------
    # 3. Collect connected components
    # -----------------------------------------------------------------------
    components: Dict[int, List[int]] = defaultdict(list)
    for i in range(n):
        components[_find(i)].append(i)

    n_merged  = sum(1 for v in components.values() if len(v) > 1)
    n_single  = len(components) - n_merged
    logger.info(
        f"[consolidate] {len(components)} component(s): "
        f"{n_merged} merged, {n_single} singletons"
    )

    # -----------------------------------------------------------------------
    # 4. Build merged partitions + impute missing values
    # -----------------------------------------------------------------------
    vm_out = value_matrix.copy()
    new_partitions: Dict[str, List] = {}
    merge_log: Dict[str, Any] = {}

    for root, member_idxs in components.items():
        source_pids = [pids[i] for i in member_idxs]

        # Singleton — pass through unchanged
        if len(member_idxs) == 1:
            pid = source_pids[0]
            new_partitions[pid] = list(partitions[pid])
            merge_log[pid] = {
                "source_partitions": [pid],
                "n_imputed_cells":   0,
                "n_groups":          len(partitions[pid]),
                "n_cols":            len(col_sets[pid]),
            }
            continue

        # Union of groups and columns
        all_groups: List = []
        for pid in source_pids:
            all_groups.extend(partitions[pid])

        union_cols: FrozenSet[str] = frozenset().union(
            *[col_sets[pid] for pid in source_pids]
        )

        logger.info(
            f"  merging {source_pids} -> "
            f"{len(all_groups)} groups x {len(union_cols)} cols"
        )

        # Ensure all union columns exist in the working matrix
        for col in union_cols:
            if col not in vm_out.columns:
                vm_out[col] = np.nan

        # Per-column median imputation
        n_imputed = 0
        for col in union_cols:
            # Groups in this component that *naturally* have this column
            natural_groups = [
                g
                for pid in source_pids
                if col in col_sets[pid]
                for g in partitions[pid]
                if g in vm_out.index
            ]
            # Compute median from natural (non-NaN) values only
            natural_vals = vm_out.loc[natural_groups, col].dropna()
            median_val = float(natural_vals.median()) if len(natural_vals) > 0 else np.nan

            if np.isnan(median_val):
                continue   # nothing to impute

            # Rows in this component that are missing this column
            missing_mask = vm_out.loc[all_groups, col].isna()
            missing_groups = missing_mask[missing_mask].index.tolist()
            if missing_groups:
                vm_out.loc[missing_groups, col] = median_val
                n_imputed += len(missing_groups)

        # Deterministic merged ID
        merged_pid = (
            f"{len(all_groups)}rows_{len(union_cols)}cols_"
            + hashlib.md5(
                ",".join(sorted(source_pids)).encode()
            ).hexdigest()[:8]
        )
        new_partitions[merged_pid] = all_groups
        merge_log[merged_pid] = {
            "source_partitions": source_pids,
            "n_imputed_cells":   n_imputed,
            "n_groups":          len(all_groups),
            "n_cols":            len(union_cols),
        }

    logger.info(
        f"[consolidate] {n} -> {len(new_partitions)} partition(s) "
        f"(imputed cells: "
        f"{sum(v['n_imputed_cells'] for v in merge_log.values())})"
    )
    return new_partitions, vm_out, merge_log
