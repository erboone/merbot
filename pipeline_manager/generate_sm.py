from pathlib import Path
import pandas as pd
from pipeline_manager import load_config

SCHEMA_PATH = "snakerules.tsv"

class SM_Rule:
    class SM_target:
        def __init__(self, type:str, targets:str): 
            self.type:str = type 
            self.targets:list[str] = targets.split(sep=',')
        def __repr__(self): 
            return f"\t{self.type}:\n\t\t\'{"\',\n\t\t\'".join(self.targets)}XXX'\n"

    class SM_code:
        pass
            
    
    def __init__(self, row:pd.core.frame.pandas):
        self.notebook:str = self.SM_target('notebook', row[1])
        self.input:SM_Rule.SM_targets = self.SM_target('input', row[2])
        self.output:SM_Rule.SM_targets = self.SM_target('output', row[3])

    def __repr__(self) -> str: return \
        f"rule {self.notebook.targets[0]}:\n{self.input!r}{self.output!r}{self.notebook!r}\n\n"
    

def load_snakerules(path:str=SCHEMA_PATH):
    rule_objs = []
    rule_df = pd.read_csv(path, sep='\t', header=0)
    print(type(rule_df))
    for row in rule_df.itertuples():
        rule_objs.append(
           SM_Rule(
               row
           )
        )

    return rule_objs


def write_snakefile(conf_path:Path):
    # TODO: next step here
    config = load_config(conf_path)
    IOO = config['IO Options']


    with open(outpath, 'w') as new_snakefile:
        for obj in load_snakerules():
            new_snakefile.write(str(obj))

def _check_target(targ:str, check_blank:bool=False) -> bool:
    pass

def check_valid_config_IO(config_path:str):
    pass

def build_snakefile(expiriemnt):
    pass
