import os 
from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

PACKAGE_DIR = f"{os.environ['HOME']}/merbot"

# Figure out a way to make sure this points to the right place no matter where it is installed
CONFIG_PATH = f"{PACKAGE_DIR}/config.master.ini"

# Error messages

def load_config(config_file:str=CONFIG_PATH) -> ConfigParser:
    config = ConfigParser()
    with open(config_file, "r") as conf_file_conn:
        config.read_file(conf_file_conn)
        if not config.sections():
            raise RuntimeError("passed config file connection has failed or does not have any sections")
        return config

MASTER_CONFIG = load_config(CONFIG_PATH)

# TODO: install to a specific directory in root
db_path = f"{PACKAGE_DIR}/{MASTER_CONFIG['Master']['experiment_db']}"
DB_ENGINE = create_engine(f"sqlite:///{db_path}")
SESSION = Session(DB_ENGINE)
