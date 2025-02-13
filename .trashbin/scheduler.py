from pipeline_manager import MASTER_CONFIG, SESSION, DB_ENGINE, load_config
from pipeline_manager.expdb import MerscopeDirectory, Experiment, Run 
from pipeline_manager.expdb import initialize_experiment_db
from pipeline_manager.smgenerator import load_pipeline
# TODO: Figure out where to put the PARSER file
# from .parser import PARSER
import parser
import json

import os, sys
import subprocess
from pathlib import Path
from configparser import ConfigParser

# Run an experiment
# TODO: probably delete this: unnecessary and makes code unreadable.
# def _check_path(path:Path, return_detail:bool=False) -> bool:
#     exists=False; read=False; write=False; 
#     if os.access(path, os.F_OK): exists = True
#     if os.access(path.parent, os.R_OK): read = True
#     if os.access(path.parent, os.W_OK): write = True
#     if return_detail:
#         return exists, read, write
#     else:
#         return all([exists, read, write])
                
    # raise RuntimeError(f"You do not have read/write permissions in directory \"{path}\"")
    #else:
    # raise RuntimeError(f"Attempted to create \"{path}\" but failed. Check that you have permission.")

# def run pipeline
# in: experiment(name)
# does:
    # checks existance of config file
        # if not exists create
def run_pipeline(
    exp:Experiment, 
    mast_conf_path:str=None, 
    target:str='full_pipeline',
    cores:int=8):
    # TODO: make a custom template and/or chance to sphinx autodoc format
    """Main command to run a pipeline on an experiment that has been logged in 
    the experiment database. Includes options to specify alternative config 
    file path and snakemake pipeline target name.

    Args:
        exp (Experiment): Experiment to run (object retreived from database)
    
    Optional:
        mast_conf_path (str): . Defaults to hlpr.CONFIG_PATH.
        target (str): _description_. Defaults to 'all'.
        cores (int): _description_. Defaults to 2.
    """


    # Look for config template
    if mast_conf_path is not None:
        mast_conf = load_config(mast_conf_path)
    else:
        mast_conf = MASTER_CONFIG
    
    conf_path, snake_path = setup(exp, mast_conf)
    conf = load_config(conf_path)
    
    # TODO: should this be run every time the pipeline is run?
    load_pipeline(conf_path).write_sf()

    # TODO: consider what works best here: should we use target files or snakemake target names
    # if not (target == 'all'): target = conf['IO Options'][target]
    sub_env = os.environ.copy()
    # sub_env["expname"] = exp.name
    sub_env["con_path"] = conf_path
    sub_env["run_id"] = str(123456789)

    with open(conf["Master"]['run_log'], 'wr')  as runlog_file:
        runlog_file.readline()
        print()
    
    # TODO: add nohup to this command
    snakemake_command = ' '.join([
        f"snakemake {target}",
        f"--snakefile {snake_path}",
        f"--cores {str(cores)}",
         "--dry-run"
    ])

    print(snakemake_command)
    subprocess.run(snakemake_command, 
                   shell=True, env=sub_env)

def getlogno(config:ConfigParser):
    runlog_path = Path(config["Master"]['run_log'])
    if not runlog_path.exists():
        
        with open(runlog_path, 'w') as new_runlog_file:
            json.dump(config, fp=new_runlog_file)
    # with open(runlog_path, 'r+')  as runlog_file:
        


def setup(exp:Experiment, mast_conf:ConfigParser) -> tuple[str, str]:
    if exp.nname is not None:
        name = exp.nname
    else: name = exp.name
    
    # check and create data analysis folder -----------------------------------
    ANAPRE_DIR_PATH = Path(mast_conf['Master']['analysis_prefix'], name)
    os.makedirs(ANAPRE_DIR_PATH, exist_ok=True)
    
    # check and create config file --------------------------------------------
    CONFIG_COPY_PATH = Path(ANAPRE_DIR_PATH, f'config.ini')
    
    if not os.access(CONFIG_COPY_PATH, os.F_OK):
        conf_copy = exp._build_config(CONFIG_COPY_PATH)
        with open(CONFIG_COPY_PATH, "w") as config_copy_file:
            conf_copy.write(config_copy_file)
    
    # Check and create snakemake file -----------------------------------------
    SNAKEMAKE_PATH = Path(ANAPRE_DIR_PATH, f'snakefile')

    # TODO: consider putting this in the 'write snakemake' function
    # TODO: consider when snakemake file has to be overwritten
    if not os.access(SNAKEMAKE_PATH, os.F_OK): # create snakemake file
        print("This is reached")
        write_snakefile(CONFIG_COPY_PATH)
    
    return CONFIG_COPY_PATH, SNAKEMAKE_PATH

def write_conf_copy(exp):
    pass

# DOWN HERE: select which experiments to run


if __name__ == "__main__":
    # args = PARSER.parse_args(sys.argv[1:])

    test_exp = Experiment.getallfromDB()[0]
    getlogno(load_config())
    # run_pipeline(test_exp)