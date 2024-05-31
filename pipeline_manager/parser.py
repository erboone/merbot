"""_summary_
"""
import argparse

PARSER = argparse.ArgumentParser(
    prog='Merfish Pipeline',
    description='Run the Quan lab merfish analysis pipeline with minimal human intervention and maximal control and transparancy',
    # TODO: do we need an epilog?
    epilog=''
)

## template
# PARSER.add_argument('filename')           # positional argument
# PARSER.add_argument('-c', '--count')      # option that takes a value
# PARSER.add_argument('-v', '--verbose', action='store_true')  # on/off flag

# Documentation
# https://docs.python.org/3/library/argparse.html

_UNFILLED_FIELD = 'Tell Eric he needs to write this'


PARSER.add_argument(
    'Expiriment',
    nargs='*', # Collects as many expiriments as are listed
    required=True,
    help=_UNFILLED_FIELD
)

PARSER.add_argument(
    
)

