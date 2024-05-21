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
from configparser import ConfigParser

# -----------------------------------------------------------------------------
# Load config file
EXP_NAME = os.environ["sm_expname"]
CONFIG_FILE = os.environ["sm_conpath"]
config = ConfigParser()
with open(CONFIG_FILE, "r") as conf_file_conn:
    config.read_file(conf_file_conn)

targets = config["IO Options"]

# Define targets --------------------------------------------------------------
end_targets = ['cell_by_gene_tab']
end_target_paths = [targets[trgt] for trgt in end_targets]

print(end_targets)
    # print(f"{targets['done_flag']}")

rule full_pipeline:
    input:
        end_targets
    output:
        "complete.txt"
    shell:
        "echo 'finished' > {output}"

rule Cell_Segmentation:        
    input:
        barcodes=targets['barcodes'],
        img_folder=targets['img_folder']
    output:
        cbgt=targets['cell_by_gene_tab']
    shell:
        """
        echo {input.barcodes}
        echo {input.img_folder}
        echo {output.cbgt}
        echo "THis works! Run {RUN.id}" > test.txt
        """
    
    # notebook:
        "1_Cell_Segmentation.ipynb"