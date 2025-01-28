
from os.path import join, exists
from os import makedirs

# DIRECTORIES

def get_dirs(root: str, dataset_id: str):
    DATA_DIR = join(root,"data")
    BASE_DIR = join(DATA_DIR,"processed",dataset_id)
    # assert exists(BASE_DIR), AssertionError(f"BASE dir {dataset_id} doesn't exist")
    INPUT_DIR = join(DATA_DIR,"input")
    OUTPUT_DIR = join(BASE_DIR,"output")
    INTERMEDIATE_DIR = join(DATA_DIR,"intermediate") # to remove in the future
    CONTROL_DIR = join(BASE_DIR,"control")
    return INPUT_DIR,OUTPUT_DIR,INTERMEDIATE_DIR,CONTROL_DIR

# TARGET
target_filename = "y_ist.csv"
target_trimmed_filename = "y_trimmed.csv"
target_sep = ","
target_design_label = "SapNummer"
target_version_label = "Version"
target_design_version_label = "SAP_Version"
target_batch_label = "WA"
target_id_label = "id"
target_value_label = "Value"
target_time_label = "CreateDate"
target_pos_label = "Zyklus"


# INPUT
input_design_label = "SAP"
input_version_label = "Version"
input_variable_label = "Variable"
input_process_label = "Process"
input_batch_label = "WA"
input_step_label = "PaPos"
input_id_label = "id"
input_abs_pos_label = "AbsPos"
input_given_label = "Given"
input_value_label = "Value"
input_time_label = "Time"


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

# TRANSVERSAL LABELS
trans_parameter_label = "parameter"
trans_value_label = "value"
trans_value_norm_label = "value_norm"
trans_position_label = "position"
trans_date_label = "date"
trans_batch_label = "batch"
trans_process_label = "process"
trans_variable_label = "variable"
trans_id_label = "id"

# TRANSVERSAL FILES
trans_missing_batches = "missing_batches.json"
trans_df_process_raw = "df_process_raw.csv"
trans_df_input_short = "df_input_short.parquet"
trans_df_input = "df_input.parquet"
time_components_labels = ["Year","Month","Day","Hour","Minute"]

# OUTPUT DATASET FILES
input_ds_label = "X.npy"
trg_ds_label = "Y.npy"


