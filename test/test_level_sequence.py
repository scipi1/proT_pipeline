import pandas as pd
from os.path import dirname, join, abspath
import sys
ROOT_DIR = dirname(dirname(abspath(__file__)))
sys.path.append(ROOT_DIR)

from process_pipeline.data_loader import get_processes
from process_pipeline.labels import *
from process_pipeline.level_sequences import *

INPUT_DIR = join(ROOT_DIR,"data/input/")
INTERMEDIATE_DIR = join(ROOT_DIR,"data/intermediate/")
TEST_DIR = join(ROOT_DIR,"data/test/")
filename_sel  = join(INTERMEDIATE_DIR, "lookup_selected.xlsx")

#load processes
_, processes = get_processes(INPUT_DIR,filename_sel)

#load processes
df_pc = pd.read_csv(join(INTERMEDIATE_DIR,"x_prochain.csv"), sep=",")

templates_dict = get_template(df=df_pc,processes=processes)


sel_template = templates_dict[453828]["B"]

df_templates = None
    
        

df_template = pd.DataFrame.from_dict({(i,j): sel_template[i][j] 
                           for i in sel_template.keys() 
                           for j in sel_template[i].keys()},orient="index")


df_lev,max_len,templates = level_sequences(df=df_pc,processes=processes,save_dir_templates=TEST_DIR)
