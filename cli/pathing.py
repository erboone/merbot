from typing import Any
from datadispatch.orm import Experiment, SESSION
import sqlalchemy as sql  
from sqlalchemy import select, update

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