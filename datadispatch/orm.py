# Eric Boone: 3/29/2024
# This file contains all the methods and classes needed for easy interfacing 
# with the experiment manager's experiment database. The database is a simple
# SQLite3 relational database (see table definitions for table schema) 
#
# Abreviations for readability:
# rootdir | rootDIR | ms_dir == MERSCOPE directory, the directory we can expect to
#                           find the raw output of a MERscope experiment.
#  
# exp                    == experiment; meaning one run of the vizgen machines
#                           that has been output to an rootDIR

# Database access
from sqlalchemy import select, update
from ._constants import MASTER_CONFIG, SESSION, DB_ENGINE
from .pulldown import assemble_metadata_df

# Object declarations
import os, copy
import re
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime
from typing import List, Optional
from sqlalchemy import String, Integer, DateTime, Boolean, BLOB
from sqlalchemy import ForeignKey, Column
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# -----------------------------------------------------------------------------
# open config file to build engine URL and connect
master_config = MASTER_CONFIG['Master']

# -----------------------------------------------------------------------------
# Define the objects under the sqlalchemy object relational mapping (ORM) 
# system. Allows for easy interface to objects stored in the database.
# Note that this is based on SQLalchemy 1.4 and is out of date with modern
# best practices.

# declatative_base class for inheritance. 
class Base(DeclarativeBase):
    
    @classmethod
    def getallfromDB(cls):
        # selects entire table from merscope_dir table without filtering
        db_objs: List[cls] = SESSION.scalars(select(cls)).all()
        return db_objs

class RootDirectory(Base):

    __tablename__ = "root_dirs"

    path: Mapped[str] = mapped_column('root', String(512), nullable=False, primary_key=True)
    format: Mapped[str] = mapped_column('format', String(512), nullable=False)
    init_dt: Mapped[str] = mapped_column('init_dt', DateTime, nullable=False, default=datetime.now())
    # I've turned these to be nullable. They should be dropped and all dir 
    # structure handled by mftools 
    experiments:Mapped[List["Experiment"]] = relationship('Experiment', back_populates='root')

    def get_outer_experiments(self, return_obj:bool=False):
        """
        Gets the experiment names elsewhere in the database. This exists to 
        allow redudency for experiments, as long as they are in different 
        directories.
        """
        stmt = select(Experiment).where(Experiment.rootdir != self.path)
        db_objs: List[Experiment] = SESSION.scalars(stmt).all()
        if return_obj:
            return db_objs
        else:
            return [e.name for e in db_objs]


class Experiment(Base):

    __tablename__ = "experiments" 
    
    exp_id = mapped_column('exp_id', Integer, nullable=False, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column('name', nullable=False)
    metakey:Mapped[str] = mapped_column('metakey', ForeignKey("metadata.name"), nullable=True)
    meta: Mapped["Metadata"] = relationship(lazy='joined')
    nname: Mapped[str] = mapped_column('nickname', nullable=True, default=None)
    # TODO: overhaul the backup system
    backup: Mapped[bool] = mapped_column('redundant', Boolean, nullable=False, default=False)

    rootdir: Mapped[str] = mapped_column('rootdir', ForeignKey("root_dirs.root"), nullable=False)
    root:Mapped["RootDirectory"] = relationship(lazy='joined')

    @classmethod
    def getallfromDB(cls, 
                     incl_original:bool=True,  
                     incl_backups:bool=False):
        # Get all experiments
        if incl_original and incl_backups:
            db_objs: List[cls] = SESSION.scalars(select(cls)).all()

        # Get either just nonbackups or just backups  
        # TODO: while I think this is robust, the logic is strange, make sure this is tested well.
        elif incl_original or incl_backups:
            stmt = select(cls).where(
                    cls.backup == (not incl_original) or 
                    cls.backup == (incl_backups)
                )
            db_objs: List[Experiment] = SESSION.scalars(stmt).all()

        elif not incl_original and not incl_backups:
            raise RuntimeError("One of params 'incl_nonredundant' and 'incl_redundant' must be true.")

        return db_objs
    
    def has_nname(self) -> bool: return self.nname is not None
    
    # def set_nname(self, new_nname:str, session:Session) -> None
    #     stmt = update()        
    
    def _build_config(self, config_path:Path) -> ConfigParser:
        
        conf:ConfigParser = copy.deepcopy(MASTER_CONFIG)
        # --- Set experiment meta-info ---
        sect = conf["Experiment"] 
        sect['name'] = self.name

        # --- Configure IO options --- 
        io = conf["IO Options"]
        ms_dir_obj:RootDirectory = self.root
        
        io['rootdir'] = self.rootdir
        # TODO: Not sure what to do with the 'experiment' field in the config file. Deleting it seems rash, but due to rootdir 
        # structure it can't meaningfully be represented as an absolute string path which is the whole point of the IO section
        #io['experiment'] = Path(self.rootdir, self.name)
        io['ms_raw_data'] = str(Path(self.rootdir, ms_dir_obj.raw_dir, self.name))
        io['ms_output'] = str(Path(self.rootdir, ms_dir_obj.output_dir, self.name))
        io['analysis_dir'] = str(Path(master_config['analysis_prefix'], self.name))
        io['config'] = str(config_path)
        io['snake'] = str(Path(io['analysis_dir'], 'snakefile'))
        
        # Baeline files
        io['img_folder'] = str(Path(io['ms_raw_data'], io['img_folder']))
        io['barcodes'] = str(Path(io['ms_output'], io['barcodes']))

        # Cellpose output
        io['cellpose'] = str(Path(io['analysis_dir'], io['cellpose']))
        io['masks'] = str(Path(io['cellpose'], io['masks']))
        io['cell_by_gene_tab'] = str(Path(io['cellpose'], io['cell_by_gene_tab']))

        # Quality control and filtering
        io['qc'] = str(Path(io['analysis_dir'], io['qc']))

        return conf
    
    def __repr__(self):
        class_info = "%10s\t%10s" % (self.name, [self.runs])
        return class_info

class Metadata(Base):
    __tablename__ = "metadata" 
    
    ExperimentName:Mapped[str] = mapped_column('name', String(128), nullable=False, primary_key=True) # Use this a a foreign key
    experiments = relationship('Experiment', back_populates='meta')

    SampleID:Mapped[str] = mapped_column(nullable=True)
    Region:Mapped[str] = mapped_column(nullable=True)
    Protocol:Mapped[str] = mapped_column(nullable=True)
    GenePanel:Mapped[str] = mapped_column(nullable=True)
    RIN:Mapped[str] = mapped_column(nullable=True)
    BICANExperimentID:Mapped[str] = mapped_column(nullable=True)
    MERFISHExperimentID:Mapped[str] = mapped_column(nullable=True)
    ExperimentStartDate:Mapped[str] = mapped_column(nullable=True)
    MeanTSCPofRegions:Mapped[str] = mapped_column(nullable=True)
    MedianTranscriptperCell:Mapped[str] = mapped_column(nullable=True)
    MedianGeneperCell:Mapped[str] = mapped_column(nullable=True)
    Instrument:Mapped[str] = mapped_column(nullable=True)
    AddNotes:Mapped[str] = mapped_column(nullable=True)
    TissueType:Mapped[str] = mapped_column(nullable=True)
    SampleThickness:Mapped[str] = mapped_column(nullable=True)
    ExperimentSuccess:Mapped[str] = mapped_column(nullable=True)
    VerificationExperimentID:Mapped[str] = mapped_column(nullable=True)
    ImagingDate:Mapped[str] = mapped_column(nullable=True)
    Notes:Mapped[str] = mapped_column(nullable=True)
    Region0:Mapped[str] = mapped_column(nullable=True)
    Region1:Mapped[str] = mapped_column(nullable=True)
    Region2:Mapped[str] = mapped_column(nullable=True)
    Region3:Mapped[str] = mapped_column(nullable=True)
    MeanofRegions:Mapped[str] = mapped_column(nullable=True)
    
    Project:Mapped[str] = mapped_column(nullable=True)
    
# TODO: this here      
# class Reference(Base):
#     exp_id = mapped_column('ref_id', Integer, nullable=False, primary_key=True, autoincrement=True)
#     name: Mapped[str] = mapped_column('name', String(128), nullable=False)

class ParamLog(Base):

    __tablename__ = "param_log" 
    
    id: Mapped[int] = mapped_column('id', Integer, nullable=False, primary_key=True, autoincrement=True)
    runname: Mapped[str] = mapped_column('runname', String, nullable=False)
    step: Mapped[str] = mapped_column('step', String, nullable=False)
    hash: Mapped[str] = mapped_column('hash', String, nullable=False)
    config: Mapped[str] = mapped_column('config', String)
    used: Mapped[str] = mapped_column('used', DateTime, nullable=False, default=datetime.now())
    success: Mapped[bool] = mapped_column('success', Boolean, nullable=True, default=False)

# class Checkpoint(Base):
#     # TODO: figure out how not to include in db
#     __tablename__ = "checkpoints_base"

#     id: Mapped[int] = mapped_column('chkpt_id', Integer, nullable=False, primary_key=True, autoincrement=True)
#     run_id: Mapped[int] = mapped_column('run_id', Integer, ForeignKey("runs.id"))
#     parent_run = relationship("Run")

#     start: Mapped[DateTime] = mapped_column('start_time', DateTime, nullable=False, default=datetime.now())
#     end: Mapped[DateTime] = mapped_column('end_time', DateTime, nullable=True, default=None)
#     complete: Mapped[bool] = mapped_column('complete', Boolean, nullable=False, default=False)

#     # TODO: add relation with before and after checkpoint to automatically create a graph

# for sect in MASTER_CONFIG.sections():
#     c = []
#     sect_keys = MASTER_CONFIG[sect].keys()

#     properties = {}
#     c.append(type(
#         f"checkpoint_{sect}",
#         (Checkpoint,),
#         {key: mapped_column(f"{key}", String(256), nullable=True, default=False) for key in sect_keys}
#     ))

