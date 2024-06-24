from pathlib import Path
import pandas as pd
from pipeline_manager import load_config
from .templates import HEADER, FIELD, RULE

SCHEMA_PATH = "snakerules.tsv"

# TODO: consider moving this to another file dedicated to defining this class
class Pipe:
    pass

    class Rule:
        wrapper = tuple[str, str]
        #TODO: define formatters for each type of entry
        class Field:
            wrap = '\'{target}\''

            def __init__(self, type:str, targets:str, defualt:str=None): 
                self.type:str = type
                if defualt is not None and pd.isna(targets):
                    self.targets:str = defualt
                else:
                    self.targets:str = targets
            
            def __repr__(self):                
                if pd.isna(self.targets):
                    return ""
                else:
                    targ_list = [self.wrap.format(target=targ) for targ in self.targets.split(sep=',')]
                    return FIELD.format(
                        type=self.type,
                        items=f",\n\t\t".join(targ_list)
                    )
        
        class Files(Field):
            wrap = 'IO_OPTS[\'{target}\']'

            def __init__(self, type:str, targets:str):
                super().__init__(type, targets)

            def __repr__(self):
                targ_list = [self.wrap.format(target=targ) for targ in self.targets.split(sep=',')]
                
                return FIELD.format(
                    type=self.type,
                    items=f",\n\t\t".join(targ_list))
        class Environment(Field):
            def __repr__():
                pass

        class Code(Field):
            def __init__(self, outer_isnt):
                self.outer:Pipe.Rule = outer_isnt
            def __repr__(self):
                return ("\tscript:\n"
                        f"\t\t\"{self.outer.notebook.targets}.py\"\n")
                
        
        def __init__(self, row:pd.core.frame.pandas):
            self.notebook:self.Field = self.Field('notebook', row[1])
            self.input = self.Files('input', row[2])
            self.output = self.Files('output', row[3])
            self.conda:self.Field = self.Field('conda', row[4], defualt="AutomatedPipeline")
            self.params:self.Field = self.Field('params', row[5], defualt="config_path=os.environ[\"sched_conpath\"]")
            
            self.code:self.Code = self.Code(outer_isnt=self)

        def __repr__(self) -> str: 
            fields_str = ""
            for f in [self.input, self.output, self.conda, self.params]:
                fields_str += str(f)
            return \
                RULE.format(
                    name=self.notebook.targets,
                    fields=fields_str
                )


def load_snakerules(path:str=SCHEMA_PATH):
    rule_objs = []
    rule_df = pd.read_csv(path, sep='\t', header=0)
    for row in rule_df.itertuples():
        rule_objs.append(
           Pipe.Rule(
               row
           )
        )

    return rule_objs


def write_snakefile(conf_path:Path):
    # TODO: next step here
    config = load_config(conf_path)
    snake_path = config['IO Options']['snake']

    # TODO: put header in a file, potentially with other sm construction info?
    # TODO: put 'sched_conpath' in a global variable so we dont have to have a bunch of floating literals
    
    with open(snake_path, 'w') as new_snakefile:
        new_snakefile.write(HEADER)
        for obj in load_snakerules():
            new_snakefile.write(str(obj))

def _check_target(targ:str, check_blank:bool=False) -> bool:
    pass

def check_valid_config_IO(config_path:str):
    pass

def build_snakefile(expiriemnt):
    pass
