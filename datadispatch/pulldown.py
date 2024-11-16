
#TODO: figure out how to remove this
import pandas as pd
import re

def update_from_gdrive():
    # Download up-to-date tsv from google drive
    print("Pulling up to date metadata from GDrive... (doesn't do anything; not implemented)")

def clean():
    # remove files just cause
    print("cleaning metadata files... (doesn't do anything; not implemented)")
    
def _helper_generate_metanames(current_col_names:iter, p=False):
    # Procuces the code to be pasted into Meatdata above
    # TODO: improve this: look into ways to dynamically generate mapped tables in sqlalchemy
    needed_cols = [re.sub(r'\([^)]*\)| |\.', '', col) for col in current_col_names]
    if p:
        for col in needed_cols:
            print(f"{col}:Mapped[str]")
    else:
        return needed_cols

def assemble_metadata_df() -> pd.DataFrame:

    METADATA_PATHS = [
        ('BICAN', "testing/BICAN_trackingsheet.tsv"),
        ('SenNet', "testing/SenNet_trackingsheet.tsv")
    ]

    meta_dfs = []
    for proj, path in METADATA_PATHS:
        new_df = pd.read_csv(path, sep='\t')
        new_cols = _helper_generate_metanames(new_df.columns)
        new_df.columns = pd.Index(new_cols)
        new_df.dropna(axis=1, how="all")
        new_df['Project'] = proj
        meta_dfs.append(new_df)

    concated = pd.concat(meta_dfs)
    concated.dropna(axis=1, how="all", inplace=True)
    concated.reset_index(inplace=True)
    
    return concated

