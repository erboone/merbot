from pathlib import Path
from typing import overload, List
from enum import Enum
import pandas as pd
from pipeline_manager import load_config
from .templates import *

SCHEMA_PATH = "snakerules.tsv"

class Pipe:
    
    def __init__(self, schema:pd.DataFrame, config):
        self.schema = schema.to_dict(orient='index')
        self.config = config
        self.rules:List[Rule] = self._parse_schema()
        self.fields:List[_Field] = [f for f in [r.fields for r in self.rules]]
    
    def __repr__(self):
        return "".join([str(r) for r in self.rules]) 

    def _parse_schema(self):
        rules = []
        for _, row in self.schema.items():
            new_rule = Rule()
            for col, string in row.items():
                enum_col = Rule.Subrule[col] 
                new_fields = Rule.init_field(type=enum_col, targets=string)
                if new_fields.__class__ is list:
                    new_rule.fields += new_fields
                else:
                    new_rule.fields.append(new_fields)
            print([type(o) for o in new_rule.fields])
            new_rule.fields.sort()
            rules.append(new_rule)
        return rules
    
    def write_sf(self):
        snake_path = self.config['IO Options']['snake']

        with open(snake_path, 'w') as new_snakefile:
            new_snakefile.write(HEADER)
            new_snakefile.write(str(self))

    
class Rule:

    class Subrule(Enum):
        step = 'name'
        input = 'input'
        output = 'output'
        params = 'params'
        env = 'conda'

    def __init__(self):
        self.fields:List[_Field] = []

    # def __init__(self, row:pd.core.frame.pandas):
    #     self.notebook:_Field = _Field('notebook', row[1])
    #     self.input = Files('input', row[2])
    #     self.output = Files('output', row[3])
    #     self.conda:_Field = _Field('conda', row[4], defualt="AutomatedPipeline")
    #     self.params:_Field = _Field('params', row[5], defualt="config_path=os.environ[\"sched_conpath\"]")
        
    #     self.code:self.Code = self.Code(outer_isnt=self)

    def __repr__(self) -> str: 
        fields_str = "".join(str(f) for f in self.fields)
        return \
            RULE.format(
                fields=fields_str
            )
        
    def init_field(type:Subrule, targets):
        match type:
            case Rule.Subrule.step:
                return [Name(type.value, targets), Script(targets)]
            case Rule.Subrule.input:
                return Files(type.value, targets)
            case Rule.Subrule.output:
                return Files(type.value, targets)
            case Rule.Subrule.params:
                return Params(type.value, targets)
            case Rule.Subrule.env:
                return Environment(type.value, targets)

        # raise RuntimeError(
        #     INIT_FIELD_ERR_MSG.format(
        #         type=type,
        #         targets=targets)
        #     )
        
class _Field:
    """
    Generic field class, not meant to be instantiated
    """
    order=-1
    wrap = '\'{target}\''
    default = None
    required = None
    
    def __init__(self, type:str, targets:str): 
        self.type:str = type
        if self.default is None and pd.isna(targets):
            self.targets = targets
        elif self.default is not None and pd.isna(targets):
            self.targets:str = self.default
        else:
            self.targets:str = targets

    def __repr__(self):                
        if pd.isna(self.targets):
            return ""
        else:
            targ_list = [self.wrap.format(target=targ) for targ in self.targets.split(sep=',')]
            if self.required is not None:
                targ_list.append(self.required)
            return FIELD.format(
                type=self.type,
                items=",\n\t\t".join(targ_list)
            )
        
    def __eq__(self, other) -> bool:
        if self.super().__class__ is other.super().__class__ :
            selftup = (self.order, self.type) 
            othertup = (other.order, other.type) 
            return selftup == othertup
        return NotImplemented

    def __lt__(self, other) -> bool :
        if super(self.__class__).__class__ is super(other.__class__).__class__ :
            selftup = (self.order, self.type) 
            othertup = (other.order, other.type) 
            return selftup < othertup
        return NotImplemented

class Name(_Field):
    order=1
    wrap = '\'{target}\''

    def __init__(self, type: str, targets: str):
        super().__init__(type, targets)

class Files(_Field):
    order=2
    wrap = 'IO_OPTS[\'{target}\']'

    def __init__(self, type:str, targets:str):
        super().__init__(type, targets)
        
class Params(_Field):
    order=3
    required = 'config_path=CON_PATH'
    def __init__(self, type:str, targets: str):
        super().__init__(type, targets)

class Environment(_Field):
    order=4
    default = 'AutomatedPipeline'

    def __init__(self, type:str, targets:str):
        super().__init__(type, targets)

class Script(_Field):
    order=5
    def __init__(self, target:str):
        target += '.py'
        super().__init__('script', target)


def load_pipeline(conf_path:Path=None, sch_path:str=SCHEMA_PATH) -> Pipe:
    rule_df = pd.read_csv(sch_path, sep='\t', header=0)
    config = load_config(conf_path)
    return Pipe(rule_df, config)

def _check_target(targ:str, check_blank:bool=False) -> bool:
    pass

def check_valid_config_IO(config_path:str):
    pass

def build_snakefile(expiriemnt):
    pass
