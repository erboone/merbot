import pandas as pd
from pipeline_manager import load_config

SCHEMA_PATH = "snakerules.tsv"

class SM_Rule:

    def __init__(self, row:pd.core.frame.pandas):
        self.notebook:str = row[1]
        self.input:list[str] = row[2]
        self.output:list[str] = row[3]

    def __repr__(self) -> str:
        rule_str = f"""\
rule {self.notebook}:
    input:
        {self.input}
    output:
        {self.output}
    shell:
        "echo 'finished' > {self.output}"\n\n\
        """
        
        return rule_str
        # this is where the rule is written into text.

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


def write_snakefile(outpath:str='snakemake'):
    with open(outpath, 'w') as new_snakefile:
        for obj in load_snakerules():
            new_snakefile.write(str(obj))

def _check_target(targ:str, check_blank:bool=False) -> bool:
    pass

def check_valid_config_IO(config_path:str):
    pass

def build_snakefile(expiriemnt):
    pass
