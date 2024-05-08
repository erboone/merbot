from expdb_classes import MerscopeDirectory, Expiriment, Run, DB_ENGINE
from expdb_manager import initialize_expiriment_db
import merfish_pipeline_helpers as hlpr

import os
import subprocess
from pathlib import Path
import argparse
from configparser import ConfigParser
from sqlalchemy import select
from sqlalchemy.orm import Session


# Run an expiriment
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
# in: expiriment(name)
# does:
    # checks existance of config file
        # if not exists create
def run_pipeline(
    exp:Expiriment, 
    mast_conf_path:str = hlpr.CONFIG_PATH, 
    target:str='all',
    cores:int=8):
    # TODO: make a custom template and/or chance to sphinx autodoc format
    """Main command to run a pipeline on an expiriment that has been logged in 
    the expiriment database. Includes options to specify alternative config 
    file path and snakemake pipeline target name.

    Args:
        exp (Expiriment): Expiriment to run (object retreived from database)
    
    Optional:
        conf_path (str): . Defaults to hlpr.CONFIG_PATH.
        target (str): _description_. Defaults to 'all'.
        cores (int): _description_. Defaults to 2.
    """

    mast_conf = hlpr.load_config(mast_conf_path)['Master']
    conf = setup(exp, mast_conf)
    if not (target == 'all'): target = conf['IO Options'][target]
    # TODO: add nohup to this command
    shell_command = f"""Snakemake 
                    --cores {cores} 
                    {target}"""
    print(shell_command)
    # subprocess.run(shell_command)


def setup(exp:Expiriment, mast_conf:ConfigParser):
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

    if not conf_ex:
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
    
    return conf_copy

# DOWN HERE: select which expiriments to run


if __name__ == "__main__":
    initialize_expiriment_db()
    session = Session(DB_ENGINE)
    test = Expiriment.getallfromDB(session)[0]
    conf = hlpr.load_config()['Master']

    run_pipeline(test)
