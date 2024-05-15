from expdb_classes import MerscopeDirectory, Experiment, Run, DB_ENGINE
from expdb_manager import initialize_experiment_db
import merfish_pipeline_helpers as hlpr

import os
import subprocess
from pathlib import Path
import argparse
from configparser import ConfigParser
from sqlalchemy import select
from sqlalchemy.orm import Session


# Run an experiment
def _check_path(path:Path, return_detail:bool=False) -> bool:
    exists=False; read=False; write=False; 
    if os.access(path, os.F_OK): exists = True
    if os.access(path.parent, os.R_OK): read = True
    if os.access(path.parent, os.W_OK): write = True
    if return_detail:
        return exists, read, write
    else:
        return all([exists, read, write])
                
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
    mast_conf_path:str = hlpr.CONFIG_PATH, 
    target:str='full_pipeline',
    cores:int=8):
    # TODO: make a custom template and/or chance to sphinx autodoc format
    """Main command to run a pipeline on an experiment that has been logged in 
    the experiment database. Includes options to specify alternative config 
    file path and snakemake pipeline target name.

    Args:
        exp (Experiment): Experiment to run (object retreived from database)
    
    Optional:
        conf_path (str): . Defaults to hlpr.CONFIG_PATH.
        target (str): _description_. Defaults to 'all'.
        cores (int): _description_. Defaults to 2.
    """

    mast_conf = hlpr.load_config(mast_conf_path)['Master']
    conf_path = setup(exp, mast_conf)
    if not (target == 'all'): target = conf['IO Options'][target]
    env_setup_command = f"""
    sm_expname = '{exp.name}'
    sm_conpath = '{conf_path}'
    """

    # TODO: add nohup to this command
    snakemake_command = f"""\
    Snakemake {target}
    --cores {cores} 
    \
    """
    print(env_setup_command)
    print(snakemake_command)

    # subprocess.run(shell_command)


def setup(exp:Experiment, mast_conf:ConfigParser) -> str:
    if exp.nname is not None:
        name = exp.nname
    else: name = exp.name
    
    # check and create data analysis folder
    ANAPRE_DIR_PATH = Path(mast_conf['analysis_prefix'], name)

    anapre_ex, anapre_r, anapre_w = _check_path(ANAPRE_DIR_PATH, return_detail=True)
    if not anapre_ex:
        if not (anapre_r and anapre_w):
            raise RuntimeError(f"You do not have read/write permissions in directory \"{ANAPRE_DIR_PATH}\"")
        os.makedirs(ANAPRE_DIR_PATH)
    
    # check and create config file
    CONFIG_COPY_PATH = Path(ANAPRE_DIR_PATH, f'config.{name}.ini')
    conf_ex, conf_r, conf_w = _check_path(CONFIG_COPY_PATH, return_detail=True)
    print("reached1", CONFIG_COPY_PATH)

    if not conf_ex:
        print("reached2")
        if not (conf_r and conf_w):
            raise RuntimeError(f"You do not have read/write permissions for \"{CONFIG_COPY_PATH}\"")
        # create config file
        conf_copy = exp._build_config(CONFIG_COPY_PATH)
        with open(CONFIG_COPY_PATH, "w") as config_copy_file:
            conf_copy.write(config_copy_file)
    else:
        if not (conf_r and conf_w):
            raise RuntimeError(f"You do not have read/write permissions for \"{CONFIG_COPY_PATH}\"")
        conf_copy = hlpr.load_config(CONFIG_COPY_PATH)
    
    return CONFIG_COPY_PATH

# DOWN HERE: select which experiments to run


if __name__ == "__main__":
    initialize_experiment_db()
    session = Session(DB_ENGINE)
    test = Experiment.getallfromDB(session)[0]
    conf = hlpr.load_config()['Master']

    run_pipeline(test)
