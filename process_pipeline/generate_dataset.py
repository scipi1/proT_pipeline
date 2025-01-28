import pandas as pd
import numpy as np
from labels import *
from os.path import dirname, join, abspath
import sys
from tqdm import tqdm


def generate_dataset(dataset_id):
    
    # define directories
    ROOT_DIR = ROOT_DIR = dirname(dirname(abspath(__file__)))
    sys.path.append(ROOT_DIR)
    _,OUTPUT_DIR,_,CONTROL_DIR = get_dirs(ROOT_DIR, dataset_id)
    filepath_target = join(CONTROL_DIR, target_filename)
    
    
    def pandas_to_numpy_ds(id_samples,df,features,initial_max_seq_len):
        
        sample_list, seq_len_list = [], []
        n_features = len(features)+1
        
        for id_ in tqdm(id_samples):
        
            # Select the rows for this ID, and convert to numpy
            sample_df = df.set_index(trans_id_label).loc[id_][features]        

            # convert to numpy
            sample_array = sample_df.reset_index().to_numpy(dtype=object)

            # save sequence length
            seq_len = len(sample_array)
            seq_len_list.append(seq_len)

            # check if initial_max_seq_len is too small
            if seq_len > initial_max_seq_len:
                ValueError("Choose larger initial_max_seq_len")

            # pad with NaN if shorter
            if seq_len < initial_max_seq_len:
                padded_array = np.full((initial_max_seq_len, n_features), np.nan, dtype=object) # create a template nan array
                padded_array[:seq_len] = sample_array  # overwrite beginning with actual data
                sample_array = padded_array

            sample_list.append(sample_array)

        # stack all sequences into one array: (# samples, sequence length, # features)
        result_array = np.stack(sample_list, axis=0)
        max_len = max(seq_len_list)

        result_array = result_array[:,:max_len]

        length_counts = pd.Series(seq_len_list).value_counts().sort_index()
        print("Found the following sequence lengths")
        print(length_counts)
        
        return result_array
    
    
    df_trg = pd.read_csv(filepath_target, sep=target_sep)
    df_input = pd.read_parquet(join(OUTPUT_DIR,trans_df_input))

    id_samples = df_input[trans_id_label].unique()
    
    input_features = [trans_process_label,
                trans_variable_label,
                trans_position_label,
                trans_value_norm_label]
    input_features.extend(time_components_labels)
    
    trg_features = [target_value_label]
    
    
    array_input = pandas_to_numpy_ds(id_samples,df_input,input_features,2000)
    array_trg = pandas_to_numpy_ds(id_samples,df_trg,trg_features,1000)
    
    np.save(join(OUTPUT_DIR,input_ds_label),array_input)
    np.save(join(OUTPUT_DIR,trg_ds_label),array_trg)
    
    
    
    
if __name__ == "__main__":
    generate_dataset(dataset_id = "dyconex_test")

