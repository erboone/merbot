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
from .pulldown import update_from_gdrive, assemble_metadata_df, clean

#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-
# create the database using ORM schema defined in "expdb_classes"
# Fill with specified Merscope directories, and check for new experiments.

def initialize_experiment_db():

    update_from_gdrive()
    _create_database()
    _initialize_merfish_dirs()
    _initialize_experiments()
    _add_tracking_sheet_metadata()
    clean()

#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-=#=-
# checks all the MERSCOPE directories listed in the config.master.ini file, 
# if they are new, adds them to the database, as well as all experiments within


def _create_database():
    Base.metadata.create_all(DB_ENGINE)


def _initialize_merfish_dirs():
    """
    Parent method to called to fill the database merscope_dirs table
    TODO: Add check to see if msdir_root exists and is accessable
    TODO: Add check/warning if MSDIR has been moved.
    """
    rootdir_locations = [
        ("MERSCOPE", json.loads(MASTER_CONFIG.get("Master", "merscope_exp_dirs"))),
        ("SMALL_MERSCOPE", json.loads(MASTER_CONFIG.get("Master", "small_merscope_exp_dirs"))),
        ("XENIUM", json.loads(MASTER_CONFIG.get("Master", "xenium_exp_dirs")))
    ]
    
    db_rootdir_objs: List[RootDirectory] = RootDirectory.getallfromDB()
    db_rootdir_paths: list[str] = [dmo.root for dmo in db_rootdir_objs]

    for format, pathlist in rootdir_locations:
        for path in pathlist:
            if path in db_rootdir_paths:
                continue
            else:                
                new_ms_dir = RootDirectory(
                    root=path,
                    format=format,
                    #raw_dir=raw_path,
                    #output_dir=out_path
                )

                SESSION.add(new_ms_dir)
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

        # Define different behavior for different technologies
        pattern = None
        translator = None # A function that converts an experiment name as it appears on the server to how it appears in metadata
        match rootdir_obj.format:
            case "MERSCOPE":
                pattern = f"{rootdir_obj.root}/*data*/*"
                translator = lambda x: x.split('_')[1]

            case "SMALL_MERSCOPE":
                pattern = f"{rootdir_obj.root}/*"
                translator = lambda x: x # Passes without transformation

            case "XENIUM":
                pattern = f"{rootdir_obj.root}/*/*"
                translator = lambda x: x # Passes without transformation

            case _:
                raise RuntimeError

        found_expir_names = [os.path.basename(path) for path in glob(pattern)]
        for new_expir_name in [n for n in found_expir_names 
                                    if n not in db_expir_names]:
            try:
                new_expir_obj = Experiment(
                    name=new_expir_name,
                    metakey=translator(new_expir_name),
                    rootdir=rootdir_obj.root,
                    # Marks redundancy if experimentname exists elsewhere in the database
                    backup=(new_expir_name in db_outer_exp_names)
                )
                SESSION.add(new_expir_obj)
                SESSION.commit()
            except IndexError:
                continue
        
        # Display all experiments found in root_directory
        print(f"{rootdir_obj.root} (format: {rootdir_obj.format})")
        print("adding:", end='\n\t')
        print(*[n for n in found_expir_names if n not in db_expir_names], sep='\n\t')
        print()



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

    meta_df = assemble_metadata_df()
    rows = meta_df.to_dict(orient='records')

    rows_not_included = 0
    import pandas as pd
    for row in rows:
        if pd.isna(row["ExperimentName"]):
            rows_not_included += 1
            continue

        new_meta_obs = Metadata(
            Project = row["Project"],
            ExperimentName = row["ExperimentName"],
            SampleID = row["SampleID"],
            Region = row["Region"],
            Protocol = row["Protocol"],
            GenePanel = row["GenePanel"],
            RIN = row["RIN"],
            BICANExperimentID = row["BICANExperimentID"],
            MERFISHExperimentID = row["MERFISHExperimentID"],
            ExperimentStartDate = row["ExperimentStartDate"],
            MeanTSCPofRegions = row["MeanTSCPofRegions"],
            MedianTranscriptperCell = row["MedianTranscriptperCell"],
            MedianGeneperCell = row["MedianGeneperCell"],
            Instrument = row["Instrument"],
            AddNotes = row["AddNotes"],
            TissueType = row["TissueType"],
            SampleThickness = row["SampleThickness"],
            ExperimentSuccess = row["ExperimentSuccess"],
            VerificationExperimentID = row["VerificationExperimentID"],
            ImagingDate = row["ImagingDate"],
            Notes = row["Notes"],
            Region0 = row["Region0"],
            Region1 = row["Region1"],
            Region2 = row["Region2"],
            Region3 = row["Region3"],
            MeanofRegions = row["MeanofRegions"],
        )
        SESSION.add(new_meta_obs)
    SESSION.commit()

    print(f"{rows_not_included} metadata rows failed validity checks")

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