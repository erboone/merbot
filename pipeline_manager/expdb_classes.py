# Eric Boone: 3/29/2024
# This file contains all the methods and classes needed for easy interfacing 
# with the experiment manager's experiment database. The database is a simple
# SQLite3 relational database (see table definitions for table schema) 
#
# Abreviations for readability:
# msdir | MSDIR | ms_dir == MERSCOPE directory, the directory we can expect to
#                           find the raw output of a MERscope experiment.
#  
# exp                    == experiment; meaning one run of the vizgen machines
#                           that has been output to an MSDIR

# Database access
from sqlalchemy import select, update
from . import MASTER_CONFIG, SESSION, DB_ENGINE

# Object declarations
import os, copy
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime
from typing import List, Optional
from sqlalchemy import String, Integer, DateTime, Boolean
from sqlalchemy import ForeignKey, Column
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# -----------------------------------------------------------------------------
# open config file to build engine URL and connect
print(os.getcwd())
CONFIG_PATH = "config.master.ini"
master_config = MASTER_CONFIG['Master']
io_config = MASTER_CONFIG['IO Options']

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

class MerscopeDirectory(Base):

    __tablename__ = "merscope_dirs"

    root: Mapped[str] = mapped_column('root', String(512), nullable=False, primary_key=True)
    init_dt: Mapped[str] = mapped_column('init_dt', DateTime, nullable=False, default=datetime.now())
    raw_dir: Mapped[str] = mapped_column('raw_dir', String(512), nullable=False)
    output_dir: Mapped[str] = mapped_column('output_dir', String(512), nullable=False)
    experiments = relationship('Experiment', back_populates='msdir_obj')

    def get_outer_experiments(self, return_obj:bool=False):
        """
        Gets the experiment names elsewhere in the database. This exists to 
        allow redudency for experiments, as long as they are in different 
        directories.
        """
        stmt = select(Experiment).where(Experiment.msdir != self.root)
        db_objs: List[Experiment] = SESSION.scalars(stmt).all()
        if return_obj:
            return db_objs
        else:
            return [e.name for e in db_objs]


class Experiment(Base):

    __tablename__ = "experiments" 
    
    exp_id = mapped_column('exp_id', Integer, nullable=False, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column('name', String(128), nullable=False)
    nname: Mapped[str] = mapped_column('nickname', String(128), nullable=True)
    # TODO: overhaul the backup system
    backup: Mapped[bool] = mapped_column('redundant', Boolean, nullable=False, default=False)
    msdir: Mapped[str] = mapped_column('msdir', String(512),  ForeignKey("merscope_dirs.root"), nullable=False)
    msdir_obj = relationship("MerscopeDirectory", back_populates="experiments")
    runs: Mapped[int] = relationship("Run", back_populates="parent_experiment",
                                            cascade="all, delete-orphan")
    
    analysis_path:Mapped[str] = mapped_column('postp_path', String(512), nullable=False, 
                                            default=f"{master_config['analysis_prefix']}/{name}")
    

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
        ms_dir_obj:MerscopeDirectory = self.msdir_obj
        
        io['msdir'] = self.msdir
        # TODO: Not sure what to do with the 'experiment' field in the config file. Deleting it seems rash, but due to msdir 
        # structure it can't meaningfully be represented as an absolute string path which is the whole point of the IO section
        #io['experiment'] = Path(self.msdir, self.name)
        io['ms_raw_data'] = str(Path(self.msdir, ms_dir_obj.raw_dir, self.name))
        io['ms_output'] = str(Path(self.msdir, ms_dir_obj.output_dir, self.name))
        io['analysis_dir'] = str(Path(master_config['analysis_prefix'], self.name))
        io['config'] = str(config_path)
        
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
        

class Run(Base):

    __tablename__ = "runs" 
    
    id: Mapped[int] = mapped_column('id', Integer, nullable=False, primary_key=True, autoincrement=True)
    exp_name: Mapped[str] = mapped_column('experiment', String(128), ForeignKey("experiments.name"))
    parent_experiment = relationship("Experiment")

    init_dt: Mapped[str] = mapped_column('init_dt', DateTime, nullable=False, default=datetime.now())

    checkpoints: Mapped[int] = relationship("Checkpoint", back_populates="parent_run",
                                            cascade="all, delete-orphan")

class Checkpoint(Base):

    __tablename__ = "checkpoints"

    id: Mapped[int] = mapped_column('chkpt_id', Integer, nullable=False, primary_key=True, autoincrement=True)
    run_id: Mapped[int] = mapped_column('run_id', Integer, ForeignKey("runs.id"))
    parent_run = relationship("Run")

    start: Mapped[DateTime] = mapped_column('start_time', DateTime, nullable=False, default=datetime.now())
    end: Mapped[DateTime] = mapped_column('end_time', DateTime, nullable=True, default=None)
    complete: Mapped[bool] = mapped_column('complete', Boolean, nullable=False, default=False)

    # TODO: add relation with before and after checkpoint to automatically create a graph