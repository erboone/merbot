import os
from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.orm import Session


CONFIG_PATH = "config.master.ini"

def load_config(config_file:str=CONFIG_PATH) -> ConfigParser:
    config = ConfigParser()
    with open(config_file, "r") as conf_file_conn:
        config.read_file(conf_file_conn)
        if not config.sections():
            raise RuntimeError("passed config file connection has failed or does not have any sections")
        return config

MASTER_CONFIG = load_config(CONFIG_PATH)

cwd = os.getcwd()
db_path = f"{cwd}/{MASTER_CONFIG['Master']['experiment_db']}"
DB_ENGINE = create_engine(f"sqlite:///{db_path}")
SESSION = Session(DB_ENGINE)
