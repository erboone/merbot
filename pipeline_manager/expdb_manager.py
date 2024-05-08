# Eric Boone: 3/29/2024
# This file contains all the methods and classes needed for easy interfacing 
# with the expiriment manager's expiriment database. The database is a simple
# SQLite3 relational database (see table definitions for table schema) 
#
# Abreviations for readability:
# msdir | MSDIR | ms_dir == MERSCOPE directory, the directory we can expect to
#                           find the raw output of a MERscope expiriment.
#  
#

import os
from pathlib import Path

from typing import List
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.orm import Session
from expdb_classes import MerscopeDirectory, Expiriment, Run, Base

import json
import merfish_pipeline_helpers as hlpr

# Load config from Master
CONFIG_PATH = "config.master.ini"
master_config = hlpr.load_config(CONFIG_PATH)['Master']

# create engine for ease of database access
cwd = os.getcwd()
db_path = f"{cwd}/{master_config['expiriment_db']}"
DB_ENGINE = create_engine(f"sqlite:///{db_path}")
# TODO: Put database engine init somewhere it can be declared once and accessed elsewhere


# adapt from helpers and put in separate file

# expiriment class

# merscope class

# initialize databases
    # got locations from config file
    # in

#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-
# create the database using ORM schema defined in "expdb_classes"
# Fill with specified Merscope directories, and check for new expiriments.

def initialize_expiriment_db():

    _create_database()
    _initialize_merscope_dirs()
    _initialize_expiriments()

        # sess.add_all([new_msdir, new_exp, new_run1, new_run2])
        # sess.commit()

    # 
    #     print(f"creating database at \'{db_path}\'")
    #     metadata_obj = MetaData()
    #     _set_msdir_schema(metadata_obj)
    #     _set_exp_schema(metadata_obj)
    #     _set_run_schema(metadata_obj)
    #     metadata_obj.create_all(db_engine)

#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-
# checks all the MERSCOPE directories listed in the config.master.ini file, 
# if they are new, adds them to the database, as well as all expiriments within


def _create_database():
    Base.metadata.create_all(DB_ENGINE)


def _initialize_merscope_dirs():
    """
    Parent method to called to fill the database merscope_dirs table
    TODO: Add check to see if msdir_root exists and is accessable
    TODO: Add check/warning if MSDIR has been moved.
    """
    config = hlpr.load_config()
    conf_msdir_paths = json.loads(config.get("Master", "merscope_dirs"))
    
    session = Session(DB_ENGINE)

    db_msdir_objs: List[MerscopeDirectory] = MerscopeDirectory.getallfromDB(session)
    db_msdir_paths: list[str] = [dmo.root for dmo in db_msdir_objs]
    print(db_msdir_paths)

    for ms_path in conf_msdir_paths:
        if ms_path in db_msdir_paths:
            continue
        else:
            raw_path, out_path = _get_merscope_subdirs(ms_path)
            print(ms_path, raw_path, out_path)
            
            with Session(DB_ENGINE) as session:
                new_msdir = MerscopeDirectory(
                    root=ms_path,
                    raw_dir=raw_path,
                    output_dir=out_path
                )

                session.add(new_msdir)
                session.commit()


def _initialize_expiriments():
    """
    Checks all MERSCOPE directories for new expiriments, adds them to the DB
    """
    # Access database loads all MERSCOPE Directory objects into a list
    session = Session(DB_ENGINE)
    db_msdir_objs: List[MerscopeDirectory] = MerscopeDirectory.getallfromDB(session)

    for ms_obj in db_msdir_objs:
        db_expir_names = [e.name for e in ms_obj.expiriments]
        db_outer_exp_names = ms_obj.get_outer_expiriments(session)
        print(db_outer_exp_names)
        found_expir_names = os.listdir(f"{ms_obj.root}/{ms_obj.raw_dir}")
        print("found : ", found_expir_names)
        # Iterate over all new expiriment names
        # Note: this implemetation allows for the same expiriment name in 
        # different Merscope Directories
        print(f"{ms_obj.root} adding exp:",[n for n in found_expir_names if n not in db_expir_names])
        for new_expir_name in [n for n in found_expir_names 
                                    if n not in db_expir_names]:
        
            new_expir_obj = Expiriment(
                name=new_expir_name,
                msdir=ms_obj.root,
                # Marks redundancy if expirimentname exists elsewhere in the database
                backup=(new_expir_name in db_outer_exp_names)
            )

            session.add(new_expir_obj)
            session.commit()


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
    # initialize_expiriment_db()

    # cwd = os.getcwd()
    # db_path = f"{cwd}/{master_config['expiriment_db']}"
    # db_engine = create_engine(f"sqlite:///{db_path}")

    # with Session(db_engine) as sess:
    #     stmt = select(Expiriment).where(Expiriment.name == "test_exp1")
    #     selected_exp = sess.execute(stmt).all()
    #     print(type(selected_exp[0]))
    initialize_expiriment_db()