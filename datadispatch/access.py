import sqlalchemy as sql  
from typing import Literal
from warnings import warn

from ._constants import SESSION
from .orm import Base, RootDirectory, Experiment, Metadata, ParamLog

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
    

def _prep_orm_object(orm_class:str | Base, return_keys=False) -> str|Base:
    if return_keys:
        return _orm_tablename(orm_class)
    elif isinstance(orm_class, str):
        return eval(orm_class)
    else:
        raise RuntimeError(f'Issue with typing: \'orm_class\' is {type(orm_class)}')

def select(orm_class:str | Base,
           where:str,
           return_keys=False):
    
    orm_object = _prep_orm_object(orm_class, return_keys)    
    conditions = eval(where)

    stmt = sql.select(orm_object).join(Metadata).join(RootDirectory).where(conditions)
    # print(stmt)
    found = SESSION.scalars(stmt).all()
    return found

def update(orm_class:str | Base,
           updates:dict,
           where:str,
           commit=True):

    conditions = eval(where)

    stmt = (
        sql.update(orm_class).
        where(conditions).
        values(**updates)
    )
    SESSION.execute(stmt)
    if commit:
        SESSION.commit()


# def insert(table:Base|str, values:dict, commit=False):

#     stmt = sql.insert(table).values(**values)
#     SESSION.execute(stmt)
#     if commit:
#         SESSION.commit()

def add(obj):
    SESSION.add_all(obj)
    SESSION.commit()

def commit():
    SESSION.commit()

if __name__ == "__main__":
    # update('Experiment', {'name':'this'})

    stmt = sql.insert(ParamLog).values(**{
            'runname': b'test',
            'step': 'test',
            'hash': '00000000',
            'config': 'test'
        })
