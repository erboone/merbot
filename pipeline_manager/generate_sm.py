from pathlib import Path
import pandas as pd
from pipeline_manager import load_config

SCHEMA_PATH = "snakerules.tsv"

class Pipe:
    pass

    class Rule:

        #TODO: define formatters for each type of entry
        class Field:
            def __init__(self, type:str, targets:str, defualt:str=None, flank:str="\'"): 
                self.type:str = type
                self.flank:str = flank
                if defualt is not None and pd.isna(targets):
                    self.targets:str = defualt
                else:
                    self.targets:str = targets
            def __repr__(self):
                f = self.flank
                if pd.isna(self.targets):
                    return ""
                else:
                    return (
                        f"\t{self.type}:\n"
                        f"\t\t{f}{"\',\n\t\t\'".join(self.targets.split(sep=','))}{f}\n"
                    )

        class Code:
            def __init__(self, outer_isnt):
                self.outer:Pipe.Rule = outer_isnt
            def __repr__(self):
                return ("\tscript:\n"
                        f"\t\t\"{self.outer.notebook.targets}.py\"\n")
                
        
        def __init__(self, row:pd.core.frame.pandas):
            self.notebook:self.Field = self.Field('notebook', row[1])
            self.input:self.Field = self.Field('input', row[2])
            self.output:self.Field = self.Field('output', row[3])
            self.conda:self.Field = self.Field('conda', row[4], defualt="config_path=os.environ[\"config_path\"]")
            self.params:self.Field = self.Field('params', row[5], defualt="config_path=os.environ[\"sched_conpath\"]", flank="")
            
            self.code:self.Code = self.Code(outer_isnt=self)

        def __repr__(self) -> str: return \
            (f"rule {self.notebook.targets}:\n"
            f"{self.input!r}"
            f"{self.output!r}"
            f"{self.params!r}"
            f"{self.conda!r}"
            f"{self.code!r}")


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
    header = (
        f"import os\n"
        f"envvars:\n"
        f"  \'sched_conpath\'\n\n"
    )
    
    with open(snake_path, 'w') as new_snakefile:
        new_snakefile.write(header)
        for obj in load_snakerules():
            new_snakefile.write(str(obj))

def _check_target(targ:str, check_blank:bool=False) -> bool:
    pass

def check_valid_config_IO(config_path:str):
    pass

def build_snakefile(expiriemnt):
    pass
