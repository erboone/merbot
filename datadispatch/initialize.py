# Eric Boone: 3/29/2024
# This file contains all the methods and classes needed for easy interfacing 
# with the experiment manager's experiment database. The database is a simple
# SQLite3 relational database (see table definitions for table schema) 
#
# Abreviations for readability:
# rootdir | MSDIR | ms_dir == MERSCOPE directory, the directory we can expect to
#                           find the raw output of a MERscope experiment.
#  
#

import os
import json
from pathlib import Path
from typing import List
from glob import glob

from .orm import RootDirectory, Experiment, Run, Metadata, Base
from ._constants import MASTER_CONFIG, SESSION, DB_ENGINE

#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-
# create the database using ORM schema defined in "expdb_classes"
# Fill with specified Merscope directories, and check for new experiments.

def initialize_experiment_db():

    _create_database()
    _initialize_merscope_dirs()
    _initialize_experiments()
    _add_tracking_sheet_metadata()

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
    conf_msdir_paths = json.loads(MASTER_CONFIG.get("Master", "merscope_exp_dirs"))
    conf_xendir_paths = json.loads(MASTER_CONFIG.get("Master", "xenium_exp_dirs"))


    
    db_msdir_objs: List[RootDirectory] = RootDirectory.getallfromDB()
    db_rootdir_paths: list[str] = [dmo.root for dmo in db_msdir_objs]

    for ms_path in conf_msdir_paths:
        if ms_path in db_rootdir_paths:
            continue
        else:
            raw_path, out_path = _get_merscope_subdirs(ms_path)
            
            new_ms_dir = RootDirectory(
                root=ms_path,
                tech='MERSCOPE',
                raw_dir=raw_path,
                output_dir=out_path
            )

            SESSION.add(new_ms_dir)
            SESSION.commit()

    # This was written after the ms ^ above. It is a more efficient 
    # implementation using glob. In the future I would like this reimplemented
    # to be unified (for extensibility and maintainence sake) 
    for xen_path in conf_xendir_paths:
        if xen_path in db_rootdir_paths:
            continue
        else:
            new_xen_dir = RootDirectory(
                root=xen_path,
                tech='XENIUM'
            )

            SESSION.add(new_xen_dir)
            SESSION.commit()


def _initialize_experiments():
    """
    Checks all MERSCOPE directories for new experiments, adds them to the DB
    """
    # Access database loads all MERSCOPE Directory objects into a list
    db_msdir_objs: List[RootDirectory] = RootDirectory.getallfromDB()

    for rootdir_obj in db_msdir_objs:
        db_expir_names = [e.name for e in rootdir_obj.experiments]
        db_outer_exp_names = rootdir_obj.get_outer_experiments(SESSION)

        match rootdir_obj.tech:
            case "MERSCOPE":
                pattern = f"{rootdir_obj.root}/*data*/*"
            case "XENIUM":
                pattern = f"{rootdir_obj.root}/*/*"
        
        found_expir_names = [os.path.basename(path) for path in glob(pattern)]
        print(f"{rootdir_obj.root}")
        # print("found: ", found_expir_names)
        # Iterate over all new experiment names
        # Note: this implemetation allows for the same experiment name in 
        # different Merscope Directories
        print("adding:", end='\n\t')
        print(*[n for n in found_expir_names if n not in db_expir_names], sep='\n\t')
        print()
        for new_expir_name in [n for n in found_expir_names 
                                    if n not in db_expir_names]:
            try:
                new_expir_obj = Experiment(
                    name=new_expir_name,
                    # TODO: This is a provisional fix. In the future, on loading metadata, search for full name matches to abbrv name in metadata table 
                    metakey=new_expir_name.split('_')[1],
                    rootdir=rootdir_obj.root,
                    # Marks redundancy if experimentname exists elsewhere in the database
                    backup=(new_expir_name in db_outer_exp_names)
                )
                SESSION.add(new_expir_obj)
                SESSION.commit()
            except IndexError:
                continue


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

def _add_tracking_sheet_metadata():
    # TODO: when initializing, this should pulldown from the google drive, for now, we'll use a local CSV
    # TODO: this shouldn't change to much, so I don't think there is a ton of need to improve past manual implementation
    CSV_FILE_PATH = 'testing/MERSCOPEExperimentLog.tsv'
    connection = DB_ENGINE.connect()

    with open(CSV_FILE_PATH, 'r') as metadata_file:
        # Discard header
        _ = metadata_file.readline()

        for line in metadata_file.readlines():
            spl_line = line.split('\t')
            new_meta_obs = Metadata(
                ExperimentName = spl_line[0],
                Invoice = spl_line[1],
                Collaborator = spl_line[2],
                ImagingDate = spl_line[3],
                SampleType = spl_line[4],
                Codebook = spl_line[5],
                TargetGenes = spl_line[6],
                ImgCartridge = spl_line[7],
                RawDataLocation = spl_line[8],
                AnalysisLocation = spl_line[9],
                OutputLocation = spl_line[10],
                Issues = spl_line[11],
                AnalyzedBy = spl_line[12],
                ImagingArea = spl_line[13],
                RunTime = spl_line[14],
                SummaryResults = spl_line[15],
                DescriptionNotes = spl_line[16]
            )
            SESSION.add(new_meta_obs)
    SESSION.commit()

    

    # df = pd.read_csv(CSV_FILE_PATH)
    # df.to_sql(name='metadata', if_exists='replace', con=connection)

    # with open(CSV_FILE_PATH, 'r') as f:    
    #     conn = DB_ENGINE.raw_connection()
    #     cursor = conn.cursor()
    #     cmd = 'COPY tbl_name(col1, col2, col3) FROM STDIN WITH (FORMAT CSV, HEADER FALSE)'
    #     cursor.execute(cmd, f)
    #     conn.commit()


if __name__ == "__main__":
    initialize_experiment_db()