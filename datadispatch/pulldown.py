
#TODO: figure out how to remove this
import pandas as pd
import re
import requests as rq
import os

from ._constants import PACKAGE_DIR

FILES = {
    'MERSCOPE_Experiment_Log':'1y4hMpNBlWS9NtRDEXgynG2cLaEwy042auDAIkfZrjRg',
    'BICAN_Tracking_Sheet':'1DyzSwODaJHShu8GeGQxIScV58_ZBmL8eX2KSUgQ5-Tc',
    'SenNet_Tracking_Sheet':'1SkoJJdWcr1hsZvgB_vBNyunmaMd4dKWmBSoq_EHuni8' 
}

# Params for google api call
DEFAULT_MIME = 'text/tab-separated-values',
KEY = 'AIzaSyAvpoJRM46FmmUh08DKjoGzTFKvjZAIHvY'
# KEY = os.environ['GOOG_API_KEY']


def update_from_gdrive():
    # Download up-to-date tsv from google drive
    # Modified from the google drive api documentation: 
    # github.com/googleworkspace/python-samples/blob/main/drive/snippets/drive-v3/file_snippet/export_pdf.py
    
    """Download a hardcoded list of files from PLT3-UCSD drive to be loaded into experiments.db:metadata.
    Returns : None
    """
    
    for file in FILES.keys():
        resp = _pull_file_from_drive(file)
        filepath = f"{PACKAGE_DIR}/datadispatch/metadata/{file}.tsv"
        with open(filepath, 'wb') as o:
            o.write(resp.content)

def _pull_file_from_drive(
        file:str, 
        params={
            'mimeType':DEFAULT_MIME,
            'key':KEY}
    ) -> rq.Response:
    fileId = FILES[file]
    try:
        resp = rq.request(
            'GET', 
            f'https://www.googleapis.com/drive/v3/files/{fileId}/export',
            params=params)

    except rq.HTTPError as error:
        print(f"An error occurred: {error}")
        file = None
    return resp


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
    meta_path = f"{PACKAGE_DIR}/datadispatch/metadata"
    METADATA_PATHS = [
        ('BICAN', f"{meta_path}/BICAN_Tracking_Sheet.tsv"),
        ('SenNet', f"{meta_path}/SenNet_Tracking_Sheet.tsv"),
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

    MERSCOPE_EXP_LOG_PATH = f"{meta_path}/MERSCOPE_Experiment_Log.tsv"
    merlog_df = pd.read_csv(MERSCOPE_EXP_LOG_PATH, sep='\t')
    merlog_df.columns = pd.Index(_helper_generate_metanames(merlog_df.columns))
    merlog_df = merlog_df.reset_index().set_index('ExperimentName')

    merlog_df.drop(
        merlog_df.columns.intersection(
            concated.columns.drop('ExperimentName')), 
        inplace=True
        )
    final_df = concated.join(merlog_df, on='ExperimentName')
    final_df.dropna(axis=1, how="all", inplace=True)
    final_df.reset_index(inplace=True)
    final_df.drop('level_0', axis=1, inplace=True)
    print(final_df)


    
    return final_df

if __name__ == '__main__':
    update_from_gdrive()
    # print(test)