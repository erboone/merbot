# Eric Boone: 3/29/2024
# This file contains all the methods and classes needed for easy interfacing 
# with the experiment manager's experiment database. The database is a simple
# SQLite3 relational database (see table definitions for table schema) 
#
# Abreviations for readability:
# msdir | MSDIR | ms_dir == MERSCOPE directory, the directory we can expect to
#                           find the raw output of a MERscope experiment.
#  
#

import os
from pathlib import Path

from typing import List
from expdb_classes import MerscopeDirectory, Experiment, Run, Base

import json
from pipeline_manager import MASTER_CONFIG, SESSION, DB_ENGINE

#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-
# create the database using ORM schema defined in "expdb_classes"
# Fill with specified Merscope directories, and check for new experiments.

def initialize_experiment_db():

    _create_database()
    _initialize_merscope_dirs()
    _initialize_experiments()

#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-
# checks all the MERSCOPE directories listed in the config.master.ini file, 
# if they are new, adds them to the database, as well as all experiments within


def _create_database():
    Base.metadata.create_all(DB_ENGINE)


def _initialize_merscope_dirs():
    """
    Parent method to called to fill the database merscope_dirs table
    TODO: Add check to see if msdir_root exists and is accessable
    TODO: Add check/warning if MSDIR has been moved.
    """
    conf_msdir_paths = json.loads(MASTER_CONFIG.get("Master", "merscope_dirs"))
    
    db_msdir_objs: List[MerscopeDirectory] = MerscopeDirectory.getallfromDB(SESSION)
    db_msdir_paths: list[str] = [dmo.root for dmo in db_msdir_objs]
    print(db_msdir_paths)

    for ms_path in conf_msdir_paths:
        if ms_path in db_msdir_paths:
            continue
        else:
            raw_path, out_path = _get_merscope_subdirs(ms_path)
            print(ms_path, raw_path, out_path)
            
            new_msdir = MerscopeDirectory(
                root=ms_path,
                raw_dir=raw_path,
                output_dir=out_path
            )

            SESSION.add(new_msdir)
            SESSION.commit()


def _initialize_experiments():
    """
    Checks all MERSCOPE directories for new experiments, adds them to the DB
    """
    # Access database loads all MERSCOPE Directory objects into a list
    db_msdir_objs: List[MerscopeDirectory] = MerscopeDirectory.getallfromDB(SESSION)

    for ms_obj in db_msdir_objs:
        db_expir_names = [e.name for e in ms_obj.experiments]
        db_outer_exp_names = ms_obj.get_outer_experiments(SESSION)
        print(db_outer_exp_names)
        found_expir_names = os.listdir(f"{ms_obj.root}/{ms_obj.raw_dir}")
        print("found : ", found_expir_names)
        # Iterate over all new experiment names
        # Note: this implemetation allows for the same experiment name in 
        # different Merscope Directories
        print(f"{ms_obj.root} adding exp:",[n for n in found_expir_names if n not in db_expir_names])
        for new_expir_name in [n for n in found_expir_names 
                                    if n not in db_expir_names]:
        
            new_expir_obj = Experiment(
                name=new_expir_name,
                msdir=ms_obj.root,
                # Marks redundancy if experimentname exists elsewhere in the database
                backup=(new_expir_name in db_outer_exp_names)
            )

            SESSION.add(new_expir_obj)
            SESSION.commit()


def _get_merscope_subdirs(path:str):
    req_subdirs = ['data', 'output']
    subdir = os.listdir(path)
    found_subdir_map = {req_sd:[sdir for sdir in subdir if req_sd in sdir] for req_sd in req_subdirs}

    for req_sd, found_sd in found_subdir_map.items():
        if len(found_sd) < 1:
            raise RuntimeError(
                f"""No \"{req_sd}\" directory found at path \"{path}\", either:
                    1. rearrange the MERSCOPE directory to match schema found in README
                    2. remove \"{path}\" from config.master.ini[Master][merscope_dirs] """)
        elif len(found_sd) > 1:
            raise RuntimeError(
                f"""Found more than one \"{req_sd}\" directory at \"{path}\", either:
                    1. rearrange the MERSCOPE directory to match schema found in README
                    2. remove \"{path}\" from config.master.ini[Master][merscope_dirs] """)
        
    return found_subdir_map['data'][0], found_subdir_map['output'][0]


if __name__ == "__main__":
    initialize_experiment_db()