from typing import Any
from datadispatch.orm import Experiment, RootDirectory, SESSION
import sqlalchemy as sql  
from sqlalchemy import select, update
import os 
from pathlib import Path

from datadispatch import access
from ._helper import parse_kvpairs
def find(**kwargs):
    # TODO: Figure out where to put input warnings/errors; here or in the merbot script
    search = kwargs['name']
    verbose = kwargs['verbose']
    stmt = sql.select(Experiment)
    found = SESSION.scalars(stmt).all()

    foundexperiments = []
    for i in found:
        if i.name and search in i.name:
            foundexperiments.append(i)
        elif i.nname and search in i.nname:
            foundexperiments.append(i)
    if len(foundexperiments) < 1:
        raise RuntimeError(f"Search of '{search}' did not find any experiments matching that pattern")

    # TODO: think of some sort of CLI option that prints but returns a list 
    # otherwise 
    print(f'Search of \'{search}\' found')
    for exp in foundexperiments:
        print(
            # TODO: format this into a nice table
            f"{exp.nname}\t{exp.name}\t{exp.rootdir}"
        )

def update(**kwargs):
    updates = kwargs['updates']
    if not isinstance(updates, dict):
        updates = parse_kvpairs(updates)

    ident = kwargs['identifier']
    where={
        'name': ident,
        'nname': ident,
        'exp_id': ident
    }
    
    access.update(
        'Experiment',
        updates=updates,
        where=where,
        wherelogic='or'
    )

def nname(**kwargs):

    updates = {'nname': kwargs['nickname']}
    where = {'name': kwargs['fullname'],
            'backup':0}
    
    access.update(
        'Experiment',
        updates=updates,
        where=where,
        wherelogic='and'
    )

    updates['nname'] += '_red'
    where['backup'] = 1

    access.update(
        'Experiment',
        updates=updates,
        where=where,
        wherelogic='and'
    )

def move(**kwargs):
    
    experimentname = kwargs['experimentname']
    destination = kwargs['destination']

    stmt = sql.select(Experiment)
    found = SESSION.scalars(stmt).all()

    for i in found:
        if i.name == experimentname or i.nname == experimentname:
            rootdir = i.rootdir     
            foundID = i.exp_id
    stmt = sql.select(RootDirectory)
    found = SESSION.scalars(stmt).all()
    for i in found:
        if i.path == rootdir:
            experimentschema = i.format
        if i.path == destination:
            typesindestination = i.format
    if experimentschema != typesindestination:
        raise RuntimeError(f"Trying to move experiment of '{experimentschema}' type into a location of '{typesindestination}' type ")
    
    elif experimentschema == "XENIUM":
        pass

    elif experimentschema == "SMALL_MERSCOPE":
        fullmovepath = Path(rootdir) / experimentname
        command = f"rsync -avh --progress '{fullmovepath}' '{destination}'"
        ifworking = os.system(command)
        if ifworking != 0:
            raise RuntimeError("Issue with the first rsync, stopping now")
        stmt = (update(Experiment).where(Experiment.exp_id == foundID).values(rootdir = destination))
        SESSION.execute(stmt)
        SESSION.commit()
        if Path(destination).exists():
            os.system(f"rm -rf '{fullmovepath}'")
    
    elif experimentschema == "MERSCOPE":
        everythingindirectory = os.listdir(rootdir)
        allanalysisdirectories = []
        alldatadirectories = []
        alloutputdirectories = []
        for item in everythingindirectory:
            checker = item.lower()
            if "analysis" in checker:
                allanalysisdirectories.append(item)
            if "data" in checker:
                alldatadirectories.append(item)
            if "output" in checker:
                alloutputdirectories.append(item)
        for a in allanalysisdirectories:
            tempanalysis = Path(rootdir) / a / experimentname
            if os.path.exists(tempanalysis):
                analysispath = tempanalysis
        for b in alldatadirectories:
            tempdata = Path(rootdir) / b / experimentname
            if os.path.exists(tempdata):
                datapath = tempdata
        for c in alloutputdirectories:
            tempoutput = Path(rootdir) / c / experimentname
            if os.path.exists(tempoutput):
                outputpath = tempoutput
        
        destinationpath = Path(destination)
        hasanalysis = any(p.is_dir() and "analysis" in p.name.lower() for p in destinationpath.iterdir())
        hasdata = any(p.is_dir() and "data" in p.name.lower() for p in destinationpath.iterdir())
        hasoutput = any(p.is_dir() and "output" in p.name.lower() for p in destinationpath.iterdir())
        if not hasanalysis:
            (destinationpath / "analysis").mkdir(parents=True, exist_ok=True)
        if not hasdata:
            (destinationpath / "data").mkdir(parents=True, exist_ok=True)
        if not hasoutput:
            (destinationpath / "output").mkdir(parents=True, exist_ok=True)

        analysisdestination = next(p for p in Path(destination).glob("*analysis*") if p.is_dir())
        if destination == "mnt/merfish15/MERSCOPE":
            datadestination = "mnt/merfish15/MERSCOPE/data"
        else:
            datadestination = next(j for j in Path(destination).glob("*data*") if j.is_dir())
        if destination == "mnt/merfish15/MERSCOPE":
            outputdestination = "mnt/merfish15/MERSCOPE/output"
        else:
            outputdestination = next(k for k in Path(destination).glob("*output*") if k.is_dir())
        
        if "analysispath" in locals():
            analysiscommand = f"rsync -avh --progress '{analysispath}' '{analysisdestination}'"
            ifanalysisworking = os.system(analysiscommand)
            if ifanalysisworking != 0:
                raise RuntimeError("Issue with the first rsync for analysis, stopping now")
        datacommand = f"rsync -avh --progress '{datapath}' '{datadestination}'"
        ifdataworking = os.system(datacommand)
        if ifdataworking != 0:
            raise RuntimeError("Issue with the first rsync for data, stopping now")
        outputcommand = f"rsync -avh --progress '{outputpath}' '{outputdestination}'"
        ifoutputworking = os.system(outputcommand)
        if ifoutputworking != 0:
            raise RuntimeError("Issue with the first rsync for output, stopping now")

        stmt = (update(Experiment).where(Experiment.exp_id == foundID).values(rootdir = destination))
        SESSION.execute(stmt)
        SESSION.commit()

        if "analysispath" in locals() and Path(analysisdestination).exists():
            os.system(f"rm -rf '{analysispath}'")
        if Path(datadestination).exists():
            os.system(f"rm -rf '{datapath}'")
        if Path(outputpath).exists():
            os.system(f"rm -rf '{outputpath}'")
