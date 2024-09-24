import sqlalchemy as sql  
from typing import Literal
from warnings import warn

from .. import SESSION
from .orm import Base, MerscopeDirectory, Experiment, Run, Checkpoint

# TODO: include other options
comp_opts = Literal['==', '!=']
log_opts = Literal['and', 'or']

def _orm_tablename(orm_class:str|Base):
    if isinstance(orm_class, str):
        # TODO: error handling
        orm_class = eval(orm_class)

    return orm_class.__table__
    

def select(orm_class:str | Base,
           where:dict,
           wherelogic:log_opts='and',
           wherecomp:comp_opts='==',
           return_keys=False):
    
    # NOTE: Asking to select({table name}) gets rows or indecies while asking
    # for select({orm class}) returns orm objects
    
    # Handles string input maybe consider moving to a helper class if repeated thrice
    if return_keys:
        orm_object = _orm_tablename(orm_class)
    elif not isinstance(orm_class, Base):
        orm_object = eval(orm_class)
    else:
        raise RuntimeError(f'Issue with typing: \'orm_class\' is {type(orm_class)}')
    

    conditions_s = f' {wherelogic} '.join(
        # TODO: this will break as soon as other data types are introduced figure out a workaround then
        ["{}.{} {} \"{}\"".format(*[orm_class, key, wherecomp, val]) for key, val in where.items()]
    )
    conditions = eval(conditions_s)
    
    stmt = sql.select(orm_object).where(conditions)
    found = SESSION.scalars(stmt).all()
    return found

def update(orm_class:str | Base,
           updates:dict,
           where:dict,
           wherelogic:log_opts='and',
           wherecomp:comp_opts='=='):
    
    orm_object = eval(orm_class)
    
    conditions_s = f' {wherelogic} '.join(
        # TODO: this will break as soon as other data types are introduced figure out a workaround then
        ["{}.{} {} \"{}\"".format(*[orm_class, key, wherecomp, val]) for key, val in where.items()]
    )

    conditions = eval(conditions_s)

    stmt = (
        sql.update(orm_object).
        where(conditions).
        values(**updates)
    )

    SESSION.execute(stmt)

    stmt = (
        sql.update(orm_object).
        where(conditions)
    )
    test = SESSION.execute(stmt)

    SESSION.commit()

if __name__ == "__main__":
    update('Experiment', {'name':'this'})
