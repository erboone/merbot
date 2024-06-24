
HEADER = \
"""
import os
import subprocess
from configparser import ConfigParser

envvars:
	'con_path',
	'run_id'

config = ConfigParser()
CON_PATH = os.environ['con_path']
with open(CON_PATH, "r") as conf_file_conn:
	config.read_file(conf_file_conn)

RUN_ID = os.environ['run_id']
RUN_LOG = config["Master"]["run_log"]

"""

RULE = \
"""
rule {name}:
{fields}

"""

FIELD = \
"""\
	{type}:
		{items}
"""

CODE = \
"""
	run:
		with open(RUN_LOG, 'a') as rl_file:
			rl_file.write(f"{RUN_ID}::{notebook}:{dict(config[notebook])}")
		subprocess.run(f"jupyter nbconvert --to python {notebook}")
""" 

if __name__ == "__main__":
	import os
	import subprocess
	from configparser import ConfigParser

	subprocess.run()

	
