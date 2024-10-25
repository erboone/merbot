from typing import Any

from sqlalchemy import select, update

from pipeline_manager import access
from ._helper import parse_kvpairs
def find(**kwargs):
    # TODO: Figure out where to put input warnings/errors; here or in the merbot script
    search = kwargs['name']
    verbose = kwargs['verbose']

    found_exp = access.select(
        'Experiment',
        where={
            'Experiment.name':search,
            'Experiment.nname':search},
        wherelogic='or'
    )
    if len(found_exp) < 1:
        raise RuntimeError(f"Search of \'{search}\' did not find any experiments")

    # TODO: think of some sort of CLI option that prints but returns a list 
    # otherwise 
    print(f'Search of \'{search}\' found:')
    for exp in found_exp:
        print(
            # TODO: format this into a nice table
            f"{exp.nname}\t{exp.name}\t{exp.msdir}"
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