# Eric Boone: 3/29/2024
# This file contains all the methods and classes needed for easy interfacing 
# with the expiriment manager's expiriment database. The database is a simple
# SQLite3 relational database (see table definitions for table schema) 
#
# Abreviations for readability:
# msdir | MSDIR | ms_dir == MERSCOPE directory, the directory we can expect to
#                           find the raw output of a MERscope expiriment.
#  
# exp                    == expiriment; meaning one run of the vizgen machines
#                           that has been output to an MSDIR

# Database access
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
# Object declarations
import os
from pathlib import Path
from configparser import ConfigParser
from datetime import datetime
from typing import List, Optional
from sqlalchemy import String, Integer, DateTime, Boolean
from sqlalchemy import ForeignKey, Column
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

import merfish_pipeline_helpers as hlpr

# -----------------------------------------------------------------------------
# open config file to build engine URL and connect

CONFIG_PATH = "config.master.ini"
master_config = hlpr.load_config(CONFIG_PATH)['Master']
io_config = hlpr.load_config(CONFIG_PATH)['IO Options']
cwd = os.getcwd()
db_path = f"{cwd}/{master_config['expiriment_db']}"
print(f"Double check: sqlite:///{db_path}")
DB_ENGINE = create_engine(f"sqlite:///{db_path}")
# TODO: Put database engine init somewhere it can be declared once and accessed elsewhere

# -----------------------------------------------------------------------------
# Define the objects under the sqlalchemy object relational mapping (ORM) 
# system. Allows for easy interface to objects stored in the database.
# Note that this is based on SQLalchemy 1.4 and is out of date with modern
# best practices.

# declatative_base class for inheritance. 
class Base(DeclarativeBase):
    
    @classmethod
    def getallfromDB(cls, session:Session):
        # selects entire table from merscope_dir table without filtering
        db_objs: List[cls] = session.scalars(select(cls)).all()
        return db_objs

class MerscopeDirectory(Base):

    __tablename__ = "merscope_dirs"

    root: Mapped[str] = mapped_column('root', String(512), nullable=False, primary_key=True)
    init_dt: Mapped[str] = mapped_column('init_dt', DateTime, nullable=False, default=datetime.now())
    raw_dir: Mapped[str] = mapped_column('raw_dir', String(512), nullable=False)
    output_dir: Mapped[str] = mapped_column('output_dir', String(512), nullable=False)
    expiriments = relationship('Expiriment', back_populates='msdir_obj')

    def get_outer_expiriments(self, session:Session, return_obj:bool=False):
        """
        Gets the expiriment names elsewhere in the database. This exists to 
        allow redudency for expiriments, as long as they are in different 
        directories.
        """
        stmt = select(Expiriment).where(Expiriment.msdir != self.root)
        db_objs: List[Expiriment] = session.scalars(stmt).all()
        if return_obj:
            return db_objs
        else:
            return [e.name for e in db_objs]


class Expiriment(Base):

    __tablename__ = "expiriments" 
    
    exp_id = mapped_column('exp_id', Integer, nullable=False, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column('name', String(128), nullable=False)
    nname: Mapped[str] = mapped_column('nickname', String(128), nullable=True)
    # TODO: overhaul the backup system
    backup: Mapped[bool] = mapped_column('redundant', Boolean, nullable=False, default=False)
    msdir: Mapped[str] = mapped_column('msdir', String(512),  ForeignKey("merscope_dirs.root"), nullable=False)
    msdir_obj = relationship("MerscopeDirectory", back_populates="expiriments")
    runs: Mapped[int] = relationship("Run", back_populates="parent_expiriment",
                                            cascade="all, delete-orphan")
    
    analysis_path:Mapped[str] = mapped_column('postp_path', String(512), nullable=False, 
                                            default=f"{master_config['analysis_prefix']}/{name}")
    

    @classmethod
    def getallfromDB(cls, session:Session, 
                     incl_original:bool=True,  
                     incl_backups:bool=False):
        # Get all expiriments
        if incl_original and incl_backups:
            db_objs: List[cls] = session.scalars(select(cls)).all()

        # Get either nonredundant or re
        elif incl_original or incl_backups:
            stmt = select(cls).where(
                    cls.backup == (incl_original) or 
                    cls.backup == (not incl_backups)
                )
            db_objs: List[Expiriment] = session.scalars(stmt).all()

        elif not incl_original and not incl_backups:
            raise RuntimeError("One of params 'incl_nonredundant' and 'incl_redundant' must be true.")

        return db_objs
    
    def __repr__(self):
        class_info = "%10s\t%10s" % (self.name, [self.runs])
        return class_info
    
    def _build_config(self, config_path:Path) -> ConfigParser:
        
        
        conf:ConfigParser = hlpr.load_config(CONFIG_PATH)
        with conf["Expriment"] as sect:
            sect['name'] = self.name

        with conf["IO Options"] as io:
            ms_dir_obj:MerscopeDirectory = self.msdir_obj
            
            io['msdir'] = self.msdir
            # TODO: Not sure what to do with the 'expiriment' field in the config file. Deleting it seems rash, but due to msdir 
            # structure it can't meaningfully be represented as an absolute string path which is the whole point of the IO section
            #io['expiriment'] = Path(self.msdir, self.name)
            io['ms_raw_data'] = Path(self.msdir, ms_dir_obj.raw_dir, self.name)
            io['ms_output'] = Path(self.msdir, ms_dir_obj.output_dir, self.name)
            io['analysis_dir'] = Path(master_config['analysis_prefix'], self.name)
            io['config'] = config_path
            
            # Baeline files
            io['img_folder'] = Path(io['ms_raw_data'], io['img_folder'])
            io['barcodes'] = Path(io['ms_output'], io['barcodes'])

            # Cellpose output
            io['cellpose'] = Path(io['analysis_dir'], io['cellpose'])
            io['masks'] = Path(io['cellpose']. io['masks'])
            io['cell_by_gene_tab'] = Path(io['cellpose']. io['cell_by_gene_tab'])

            # Quality control and filtering
            io['qc'] = Path(io['analysis_dir'], io['qc'])

        return conf
        

class Run(Base):

    __tablename__ = "runs" 
    
    id: Mapped[int] = mapped_column('id', Integer, nullable=False, primary_key=True, autoincrement=True)
    exp_name: Mapped[str] = mapped_column('expiriment', String(128), ForeignKey("expiriments.name"))
    parent_expiriment = relationship("Expiriment")

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



# class Checkpoint(Base):
#     __tablename__ = "checkpoints" 

#     id: Mapped[int] = mapped_column('run_id', Integer, nullable=False, primary_key=True, autoincrement=True)
#     run_id: Mapped[int] = r

# TODO: scrap this for parts
# class OldMerscopeDirectory:

#     def __init__(self, root_dir:str=None, json:dict=None) -> None:
#         self.root = root_dir
#         self.date_added = datetime.now()
#         self.subdir = self._merscope_subdirs(self.root)
#         self.all_exp = self._find_all_expiriments()
#         self.incomplete_exp = []
#         self.complete_exp = []
    
#     @classmethod
#     def from_json(cls, mdir_json:dict):
#         new_ms_dir = cls(root_dir=mdir_json["root"])
#         new_ms_dir.date_added = datetime.fromisoformat(mdir_json["date_added"])
#         new_ms_dir.subdir = mdir_json["subdir"]
#         new_ms_dir.all_exp = new_ms_dir._find_all_expiriments()
#         new_ms_dir.incomplete_exp = [MerfishExpiriment.from_json(exp_json) 
#                                         for exp_json in mdir_json["incomplete_exp"].values()]
#         new_ms_dir.complete_exp = [MerfishExpiriment.from_json(exp_json) 
#                                         for exp_json in mdir_json["complete_exp"].values()]
#         new_ms_dir._initialize_expiriments()
#         return new_ms_dir

#     def __str__(self):
#         incomp_exp = "".join([f"\t{str(exp)}\n" for exp in self.incomplete_exp])
#         comp_exp = "".join([f"\t{str(exp)}\n" for exp in self.complete_exp])
#         class_info = "MERSCOPE Dir: %s\nDate added to log: %s\nIncomplete expiriments:\n%sComplete expiriments:\n%s" % (self.root, self.date_added, incomp_exp, comp_exp)
#         return class_info
#         # TODO: Write __str__ for Expiriment object and list below {class info}

#     def to_dict(self):
#         md_dict = {
#             "root": self.root,
#             "date_added": self.date_added.isoformat(),
#             "subdir": self.subdir,
#             "all_exp": self.all_exp,
#             "incomplete_exp": {expir.name: expir.to_dict() for expir in self.incomplete_exp},
#             "complete_exp": {expir.name: expir.to_dict() for expir in self.complete_exp}
#         }
#         return md_dict

#     def _merscope_subdirs(self, path:str):
#         req_subdirs = ['data', 'output']
#         subdir = os.listdir(path)
#         output_subdir = {req_sd:[sdir for sdir in subdir if req_sd in sdir] for req_sd in req_subdirs}

#         for req_sd, found_sd in output_subdir.items():
#             if len(found_sd) < 1:
#                 raise RuntimeError(
#                     f"""No \"{req_sd}\" directory found at path \"{path}\", either:
#                         1. rearrange the MERSCOPE directory to match schema found in README
#                         2. remove \"{path}\" from config.master.ini[Master][merscope_dirs] """)
#             elif len(found_sd) > 1:
#                 raise RuntimeError(
#                     f"""Found more than one \"{req_sd}\" directory at \"{path}\", either:
#                         1. rearrange the MERSCOPE directory to match schema found in README
#                         2. remove \"{path}\" from config.master.ini[Master][merscope_dirs] """)
            
#         return {req_sd: found_sd[0] for req_sd, found_sd in output_subdir.items()}
    
#     def _find_all_expiriments(self):
#         expiriments = os.listdir('/'.join([self.root, self.subdir['output']]))
#         return expiriments
    
#     def _initialize_expiriments(self, done:bool=False):
#         complete_exp_names = [exp.name for exp in self.complete_exp]
#         for exp in self.all_exp:
#             if exp not in complete_exp_names:
#                 if done:
#                     self.complete_exp.append(MerfishExpiriment(name=exp, ms_path=self.root))
#                 else:
#                     self.incomplete_exp.append(MerfishExpiriment(name=exp, ms_path=self.root))

