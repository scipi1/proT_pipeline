# import pandas as pd
# import numpy as np
# import sys
# from data_loader import get_processes,get_booking,generate_lookup
# from sequence_builder import sequence_builder
# from data_trimmer import data_trimmer
# from level_sequences import level_sequences
# from data_post_processing import data_post_processing
# from labels import *
# from argparse import ArgumentParser
# from fix_steps import fix_steps
# import json
# #from os import abspath
# from os.path import dirname, join, abspath, exists
# from os import makedirs
# import sys
from assemble_raw import assemble_raw
from process_raw import process_raw
from match_target import match_target
from generate_dataset import generate_dataset

dataset_id = "dyconex_test"
debug = True

print("Assembling raw process dataframe...")
assemble_raw(dataset_id=dataset_id, debug=debug)
print("Done!")

print("Processing raw dataframe...")
process_raw(dataset_id=dataset_id)
print("Done!")

print("Matching the target data...")
match_target(dataset_id=dataset_id)
print("Done!")

print("Generate dataset...")
generate_dataset(dataset_id)
print("Done!")