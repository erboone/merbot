import argparse as ap

Parser=ap.ArgumentParser
Subparser=ap._SubParsersAction



PARSER:ap.ArgumentParser = ap.ArgumentParser( 
                                    description = "Does a thing to some stuff.",
                                    epilog = "As an alternative to the commandline, params can be placed in a file, one per line, and specified on the commandline like '%(prog)s @params.conf'.",
                                    fromfile_prefix_chars = '@' )

subparser:Subparser = PARSER.add_subparsers(dest='subcommand')

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
# ----- Pathing subcommands -----

# --- find ---
find:Parser = subparser.add_parser(
    'find',
    help='Get path to experiment root'
)

find.add_argument(
    'name',
    default='_null_',
    help='The name or nickname to search for in the database'
)

# Can this be inherited?
find.add_argument(
    '-v', '--verbose',
    action='store_true',
    default=False
)

# update
nname:Parser = subparser.add_parser(
    'update',
    help='Get path to experiment root'
)

nname.add_argument('identifier',
    metavar='id',
    help='Full name of the experiment that will be receiving an alias'
)

nname.add_argument('updates',
    metavar="KEY=VALUE",
    nargs='+',
    help="Set a number of key-value pairs "
            "(do not put spaces before or after the = sign). "
            "If a value contains spaces, you should define "
            "it with double quotes: "
            'foo="this is a sentence". Note that '
            "values are always treated as strings."
)

# nname
nname:Parser = subparser.add_parser('nname',
    help='Get path to experiment root'
)

nname.add_argument('fullname',
    help='Full name of the experiment that will be receiving an alias'
)

nname.add_argument('nickname',
    help='The new, easy to remember alias for an experiment'
)

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
# ----- Database subcommands -----

# initialize
nname:Parser = subparser.add_parser('init',
    help='creates database'
)

# refresh
nname:Parser = subparser.add_parser('refresh',
    help='creates database'
)

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#
# ----- Config subcommands -----

