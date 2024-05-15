# SenNet Cell Segmentation Cookbook
# Authors: 
# Eric Boone since 3/1/2024

# For now this cookbook is just to set up the directories in the way the \
# pipeline expects.
# Expect that this will eventually be combined with the Cell Senescene cookbook 
# -----------------------------------------------------------------------------

import sys
import os
from pathlib import Path
from datetime import datetime 
import configparser
from pipeline_manager import SESSION
from pipeline_manager import RUN


# -----------------------------------------------------------------------------
# Load config file
CONFIG_FILE = os.environ["sm_conpath"]
config = hlpr.load_config(CONFIG_FILE)
targets = config["IO Options"]

# -----------------------------------------------------------------------------
# Create run table entry
RUN = 

# Define targets --------------------------------------------------------------
end_targets = ['cell_by_gene_tab']
end_target_paths = [targets[trgt] for trgt in end_targets]

print(end_targets)
    # print(f"{targets['done_flag']}")

rule full_pipeline:
    input:
        end_targets
    output:
        f"{runid}.{expname}.log"


rule cbgt:        
    input:
        barcodes=targets['barcodes'],
        img_folder=targets['img_folder']
    output:
        cbgt=targets['cell_by_gene_tab']
    notebook:
        "1_Cell_Segmentation.ipynb"