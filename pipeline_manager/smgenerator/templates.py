
HEADER = \
"""
import os
import subprocess
from configparser import ConfigParser

envvars:
	'conpath'
	'run_id'

config = ConfigParser()
con_path = os.environ['conpath']
with open(conpath, "r") as conf_file_conn:
	config.read_file(conf_file_conn)

RUN_ID = con_path = os.environ['run_id']
RUN_LOG = config["Master"]["run_log"]

"""

RULE = \
"""
{name}:
	{fields}
"""

FIELD = \
"""
	{type}:
		{flank}{items}{flank}

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

	
