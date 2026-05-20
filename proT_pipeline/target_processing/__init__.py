"""
Target processing module for IST (In-System Test) data.

This module processes IST resistance data to generate target dataframes (df_trg.csv)
that are used by the input processing pipeline.

Main components:
- main: Process real IST data from raw files
- modules: IST processing functions (filtering, normalization, etc.)
- placeholders: Generate placeholder targets for prediction mode
"""

from proT_pipeline.target_processing.main import main
from proT_pipeline.target_processing import modules
from proT_pipeline.target_processing.placeholders import (
    generate_ist_placeholders,
    load_group_ids_from_process_data
)
from proT_pipeline.target_processing.compute_ist_slopes import compute_ist_slopes

__all__ = [
    'main',
    'modules',
    'generate_ist_placeholders',
    'load_group_ids_from_process_data',
    'compute_ist_slopes',
]
