
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
IO_OPTS = config["IO Options"]
"""

RULE = \
"""
rule:
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

INIT_FIELD_ERR_MSG = """
\"init_field\" requires a valid literal in the \'type\' field.
Expected one of ['step', 'input', 'output', 'env', 'params'] but got '{type}'.
Targets: '{targets}'
"""

if __name__ == "__main__":
	import os
	import subprocess
	from configparser import ConfigParser

	subprocess.run()

	
