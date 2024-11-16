import sqlalchemy as sql  
from typing import Literal
from warnings import warn

from ._constants import SESSION
from .orm import Base, RootDirectory, Experiment, Metadata, Run, Checkpoint

# TODO: include other options
comp_opts = Literal['==', '!=']
log_opts = Literal['and', 'or']

def _orm_tablename(orm_class:str|Base):
    if isinstance(orm_class, str):
        # TODO: error handling
        orm_class = eval(orm_class)

    return orm_class.__table__

def _sanitize_dict(di:dict):
    new_di = di.copy()
    for key, val in di.items():
        if isinstance(val, str):
            new_di[key] = "\"" + val + "\""

    return new_di 
    

def select(orm_class:str | Base,
           where:str,
           wherelogic:log_opts='and',
           wherecomp:comp_opts='==',
           return_keys=False):
    
    # NOTE: Asking to select({table name}) gets rows or indecies while asking
    # for select({orm class}) returns orm objects
    
    # Handles string input maybe consider moving to a helper class if repeated thrice
    # TODO: this whole thing needs more reliable input handling; I need tests
    if return_keys:
        orm_object = _orm_tablename(orm_class)
    elif isinstance(orm_class, str):
        orm_object = eval(orm_class)
    else:
        raise RuntimeError(f'Issue with typing: \'orm_class\' is {type(orm_class)}')
    
    # where = _sanitize_dict(where)

    # conditions_s = f' {wherelogic} '.join(
    #     ["{} {} {}".format(*[key, wherecomp, val]) for key, val in where.items()]
    # )
    # conditions = eval(conditions_s)
    conditions = eval(where)
    print(conditions)
    print()

    stmt = sql.select(orm_object).join(Metadata).join(RootDirectory).where(conditions)
    print(stmt)
    found = SESSION.scalars(stmt).all()
    return found

def update(orm_class:str | Base,
           updates:dict,
           where:dict,
           wherelogic:log_opts='and',
           wherecomp:comp_opts='==',
           commit=True):

    
    orm_object = eval(orm_class)
    where = _sanitize_dict(where)
    
    conditions_s = f' {wherelogic} '.join(
        # TODO: this will break as soon as other data types are introduced figure out a workaround then
        ["{}.{} {} {}".format(*[orm_class, key, wherecomp, val]) for key, val in where.items()]
    )
    print()
    conditions = eval(conditions_s)

    stmt = (
        sql.update(orm_object).
        where(conditions).
        values(**updates)
    )
    print(conditions_s, "\t\t", stmt)
    SESSION.execute(stmt)
    if commit:
        SESSION.commit()

if __name__ == "__main__":
    # update('Experiment', {'name':'this'})
    res:list[Experiment] = select('Experiment', 
                              where="Experiment.rootdir_obj.format == SMALL_MERSCOPE")

