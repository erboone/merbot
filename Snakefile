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
import merfish_pipeline_helpers as pipe


# -----------------------------------------------------------------------------
# Load config file
CONFIG_FILE = "config.master.ini"
config = configparser.ConfigParser()
config.read_file(open(CONFIG_FILE, "r"))
io_conf = config["IO Options"]
master_conf = config["Master"]
EXP_LOG_FILE = master_conf["expiriment_log"]

NEW_EXPIRIMENTS = pipe.find_new_exp()[0]

# Define targets --------------------------------------------------------------
end_targets = [
        exp_config['cell_by_gene_tab'],
        exp_config['config']]

print(end_targets)
    # print(f"{targets['done_flag']}")

rule full_pipeline:
    input:
        end_targets
    output:
        f"{runid}.{expname}.log"


rule cbgt:
    params:
        
    input:
        config=f"{POSTPROCESS}" + "{sample}/config.ini",
        barcodes=targets['barcodes']
    output:
        cbgt="/home/eboone/CellSegmentation/postprocess/{sample}/cellpose/cell_by_gene.csv"
    shell:
        """
        echo 'This works!' > {output.cbgt}
        """
    # notebook:
    #     "1_Cell_Segmentation.ipynb"

rule config: 
    input:
        config_master="config.master.ini"
    output:
        config="/home/eboone/CellSegmentation/postprocess/{sample}/config.ini"
    run:
        # Load master config
        config = pipe.load_config(input.config_master)

        io_conf = config['IO Options']
        io_conf['merscope'] = targets['merscope']
        io_conf['expiriment'] = expiriment.name
        io_conf['mer_raw_data'] = targets['raw_data_dir']
        io_conf['mer_output'] = targets['output_dir']
        io_conf['barcodes'] = targets['barcodes']

        io_conf["cellpose"] = targets['cellpose_dir']
        io_conf["masks"] = targets['masks_dir']


        with open(output.config, "w") as config_copy:
            config.write(config_copy)

# -----------------------------------------------------------------------------
# Meta rules
rule status:
    input:
        config=CONFIG_FILE,
        exp_log=EXP_LOG_FILE
    output:
        status=master_conf['status']
    run:
        ms_dir_dict = pipe.read_expiriment_log()
        with open(output.status, "w") as status_outfile:
            status_outfile.write(f"Status as of {datetime.now().date()}:\n\n")
            for ms_dir_name, ms_dir in ms_dir_dict.items():
                status_outfile.write(str(ms_dir))

rule update:
    input:
        ".snakemake/comp_exp.txt"

rule initialize_Merfish_Dirs:
    input:
        "config.master.ini"
    output:
        touch(".snakemake/flags/initialize.flag")
    run:
        pipe.initialize_Merfish_Dirs()


rule setup:
    output:
        touch(".snakemake/flags/setup.flag")        for path in paths:
            os.makedirs(path, exist_ok=True)

            if os.path.exists(path):
                if not os.access(path, os.W_OK) or not os.access(path, os.R_OK):
                    raise RuntimeError(f"You do not have read/write permissions in directory \"{path}\"")
            else:
                raise RuntimeError(f"Attempted to create \"{path}\" but failed. Check that you have permission.")

    run:
        paths = hlpr.assemble_directory_paths



# Run newfound directories ----------------------------------------------------

def get_new_exp_targets():
    new_ex = [Path(master_conf['mer_postprocess'], 
                exp.name,
                master_conf['cellpose'],
                io_conf['cellpose_outfile']) for exp in pipe.find_new_exp()[0]]
    if new_ex:
        print(f"Found the following expiriments:")
        for p in new_ex:
            print(f"\t{p}")
        return new_ex
    else:
        return master_conf['status']


rule default:
    input:
        [f"{io_conf['mer_postprocess']}/{new_ex.name}/done.flag" for new_ex in NEW_EXPIRIMENTS]

# rule CellSegmentation:
#     input:
#         runtime_env=".snakemake/flags/setup.flag"
#     output:
#         # In expiriment output folder? should probably go in the MERLIN rule
#         cell_seg=f"{}cell_metadata.csv",
#         barcodes=f"{}detected_transcripts.csv",
#         cbgt=f"{}cell_by_gene.csv"
#     notebook:
#         "1_Cell_Segmentation.ipynb"
