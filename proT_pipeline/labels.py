"""
Unified labels module for proT_pipeline.

This module contains all label definitions for both:
- Target processing (IST data)
- Input processing (process data)

Shared transversal labels are used by both pipelines.
"""

from os.path import join, exists, dirname, abspath
from os import makedirs
from omegaconf import OmegaConf
import sys

# =============================================================================
# DIRECTORY FUNCTIONS
# =============================================================================

def get_root_dir():
    """
    Get the root directory of the project.
    
    Returns:
        str: Absolute path to the project root directory
    """
    return dirname(dirname(abspath(__file__)))


def get_target_dirs(root: str):
    """
    Get directories for target (IST) data processing.
    
    Args:
        root (str): Project root directory
        
    Returns:
        tuple: (INPUT_DIR, BUILDS_DIR) for IST data
    """
    DATA_DIR = join(root, "data", "target")
    INPUT_DIR = join(DATA_DIR, "input")
    BUILDS_DIR = join(DATA_DIR, "builds")
    return INPUT_DIR, BUILDS_DIR


def get_input_dirs(root: str, dataset_id: str):
    """
    Get directories for input (process) data processing.
    
    Args:
        root (str): Project root directory
        dataset_id (str): Dataset identifier
        
    Returns:
        tuple: (INPUT_DIR, OUTPUT_DIR, CONTROL_DIR) for process data
    """
    DATA_DIR = join(root, "data")
    BASE_DIR = join(DATA_DIR, "builds", dataset_id)
    assert exists(BASE_DIR), AssertionError(f"Builds directory \"{dataset_id}\" doesn't exist")
    
    OUTPUT_DIR = join(BASE_DIR, "output")
    CONTROL_DIR = join(BASE_DIR, "control")
    
    config = None
    try:
        config = OmegaConf.load(join(CONTROL_DIR, "config.yaml"))
    except:
        print("Configuration file not found")
    
    if config is not None:
        INPUT_DIR = join(DATA_DIR, "input", config["dataset"])
    else:
        INPUT_DIR = join(DATA_DIR, "input")
        
    return INPUT_DIR, OUTPUT_DIR, CONTROL_DIR


# Backward compatibility alias
def get_dirs(root: str, dataset_id: str):
    """
    Backward compatibility function. 
    Calls get_input_dirs() with the same signature.
    """
    return get_input_dirs(root, dataset_id)


# =============================================================================
# TRANSVERSAL LABELS (Shared by both pipelines)
# =============================================================================

trans_parameter_label = "parameter"
trans_value_label = "value"
trans_value_norm_label = "value_norm"
trans_position_label = "position"
trans_date_label = "date"
trans_batch_label = "batch"
trans_process_label = "process"
trans_variable_label = "variable"
trans_order_label = "order"
trans_id_label = "id"
trans_group_id = "group"
trans_design_label = "design"
trans_version_label = "version"
trans_design_version_label = "design_version"
trans_occurrence_label = "occurrence"
trans_step_label = "step"
trans_class_label = "class"
time_components_labels = ["year", "month", "day", "hour", "minute"]

# CLASS LABELS (Parameter type classification: Read vs Set)
class_read_label = "Read"
class_set_label = "Set"
class_dict_filename = "class_vocabulary.json"


# =============================================================================
# TARGET LABELS (IST Data Processing)
# =============================================================================

# TARGET FILES
target_filename_np = "Y_np.npy"
target_filename_input = "IST_Ergebniss01.csv"
target_filename_df = "df_trg.csv"
target_filename_dataframe_filter = "ist_dataframe_filter.csv"
target_filename = "df_trg.csv"  # Alias for compatibility
target_trimmed_filename = "y_trimmed.csv"
target_sep = ","

# TARGET ORIGINAL LABELS (from raw IST data)
target_original_design_label = "SapNummer"
target_original_version_label = "Version"
target_original_design_version_label = "SAP_Version"
target_original_batch_label = "WA"
target_original_name_label = "Name"
target_original_time_label = "CreateDate_1"
target_time_format = "%m/%d/%y %I:%M %p"
target_original_pos_label = "Zyklus"
target_number_cycles_label = "AnzahlZyklen_2"
target_original_id_label = "couponID"
target_original_id_name_label = "Name"
target_original_sense_A_label = "WiderstandSenseA"
target_original_sense_B_label = "WiderstandSenseB"
target_senses_list = [target_original_sense_A_label, target_original_sense_B_label]
target_original_delta_A_label = "DeltaSenseA"
target_original_delta_B_label = "DeltaSenseB"
target_original_sep = ";"
target_temperature_label = "Temperatur_2"
target_temperature_select_label = "high"

# TARGET PROCESSING LABELS
target_delta_prefix = "delta_"
target_delta_A_label = target_delta_prefix + target_original_sense_A_label
target_delta_B_label = target_delta_prefix + target_original_sense_B_label
target_deltas_list = [target_delta_A_label, target_delta_B_label]
target_norm_delta_A_label = "delta_A_norm"
target_norm_delta_B_label = "delta_B_norm"
target_stack_label_list = ["A", "B"]
target_id_label = "id"
target_type_label = "Type"
target_panel_label = "Panel"
target_letter_label = "Letter"
target_value_label = "Value"
target_stack_label = "Sense"
target__type_canary_value = "C"
target_type_product_value = "P"

# TARGET LEGACY LABELS (for compatibility)
target_design_label = "SapNummer"
target_version_label = "Version"
target_design_version_label = "SAP_Version"
target_batch_label = "WA"
target_time_label = "CreateDate"
target_pos_label = "Zyklus"

# TARGET ANALYSIS LABELS
SNR_label = "SNR"
number_cycles_label = "n_cycles"

# TARGET FILTER SETTINGS
filter_settings = {
    "stage_1": {
        SNR_label: 1, 
        number_cycles_label: 200
    },
    "stage_2": {
        SNR_label: 1,
        number_cycles_label: None
    }
}


# =============================================================================
# INPUT LABELS (Process Data)
# =============================================================================

# INPUT DATA LABELS
input_design_label = "SAP"
input_version_label = "Version"
input_variable_label = "Variable"
input_process_label = "Process"
input_batch_label = "batch"
input_step_label = "PaPos"
input_id_label = "id"
input_abs_pos_label = "AbsPos"
input_given_label = "Given"
input_value_label = "Value"
input_time_label = "Time"

# OCCURRENCE FILE LABELS
occurrence_position_label = "Pos"
occurrence_layer_label = "Lage"

# CONTROL FILES
selected_filename = "lookup_selected.xlsx"
selected_process_filename = "steps_selected.xlsx"
lookup_filename = "lookup.xlsx"
input_raw_filename = "x_prochain.csv"
input_leveled_filename = "x_prochain_lev.csv"
booking_missing_filename = "booking_missing.csv"
process_missing_filename = "process_missing.csv"
booking_design_label = "SAP"
booking_version_label = "SAP_Version"
booking_batch_label = "WA"
booking_step_label = "PaPosNumber"

# TEMPLATES
templates_filename = "templates.csv"
templates_design_label = input_design_label
templates_version_label = input_version_label
templates_step_label = input_step_label
templates_variable_label = input_variable_label

# GENERAL
standard_sep = ","

# CSV READING SETTINGS
# Controls pandas low_memory option for read_csv
# True (default): Safe for low-RAM machines, may produce dtype warnings
# False: Better dtype inference, requires more RAM
csv_low_memory = True

# TRANSVERSAL FILES
trans_missing_batches = "missing_batches.json"
var_dict_filename = "variables_vocabulary.json"
process_dict_filename = "process_vocabulary.json"
pos_dict_filename = "position_vocabulary.json"
group_dict_filename = "group_vocabulary.json"
step_dict_filename = "step_vocabulary.json"
batch_dict_filename = "batch_vocabulary.json"
feat_dict_filename = "features_dict"
trans_df_process_raw = "df_process_raw.csv"
trans_df_input_short = "df_input_short.parquet"
trans_df_input = "df_input.parquet"

# OUTPUT DATASET FILES
input_ds_label = "X.npy"
trg_ds_label = "Y.npy"
# Compressed dataset files (npz format)
full_data_label = "data.npz"
train_data_label = "train_data.npz"
test_data_label = "test_data.npz"
