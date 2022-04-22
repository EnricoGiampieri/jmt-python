"""This version of the program will parse the jsontables in a stream format
"""
# %% do imports
# import os
import re
import io
# import contextlib
# from sqlite3 import connect, Row
# from contextlib import closing
from typing import Iterable, Tuple, Union
from typing import NamedTuple
import json
from functools import partial
# import operator as op
import itertools as it
# I would rather not depend from it, but for now we can't avoid it
# import pandas as pd
# from openpyxl import load_workbook, Workbook

# %% queste sono le funzioni per leggere/scrivere in stream
import argparse
import sys
from collections.abc import Iterable as acbIterable
from contextlib import contextmanager
import fileinput

# -----------------------------------------------------------------------------
# FUNZIONI DI UTILITY
# -----------------------------------------------------------------------------

def add_stream_input(pars):
    """add a common option for optional input to a parser"""
    pars.add_argument('-i', '--input', default=sys.stdin)


def add_stream_output(pars):
    """add a common option for optional output to a parser"""
    pars.add_argument('-o', '--output', default=sys.stdout)


def iterable(obj):
    """check if the argument is a filename or standard input/output"""
    is_iterable = isinstance(obj, acbIterable)
    is_not_string = not isinstance(obj, str)
    return is_iterable and is_not_string


def datastream(stream):
    """read the lines from stdin or read line by line if it is a file """
    if iterable(stream):
        yield from stream
    else:
        with open(stream, "r", encoding="utf8") as infile:
            yield from infile


@contextmanager
def printable(stream):
    """return something on which I can write, either stdout or an open file"""
    if iterable(stream):
        yield stream
    else:
        with open(stream, "w", encoding="utf8") as outfile:
            yield outfile

"""
esempio d'uso
parser = argparse.ArgumentParser()
add_stream_input(parser)
add_stream_output(parser)
args = parser.parse_args()

for line in datastream(args.input):
    with printable(args.output) as output:
        print(line.rstrip(), file=output)
"""
# %%

class LocationData(NamedTuple):
    """Simple class to store the start and end of a line in a byte data source.

    it will be used to generate index for fast access.
    """
    start: int
    end: int
    data: Union[dict, list]

TableType = Iterable[Tuple[LocationData, Iterable[LocationData]]]

"""
def match(name:str, header: dict) -> bool:
    name_regex = re.compile(name)
    table_name = header['name']
    do_match = name_regex.match(table_name) is not None
    return do_match

def keep(name:str, tables: TableType) -> TableType:
    for header, data in tables:
        if match(name, header.data):
            yield header, data

def drop(name:str, tables: TableType) -> TableType:
    for header, data in tables:
        if not match(name, header.data):
            yield header, data
"""
# %%

# %% check for the possibility of indexing the file on a single read

# a string
# a number
# an object (JSON object)
# an array
# a boolean
# null




def parse_file(byte_stream: Iterable[bytes]) -> Iterable[LocationData]:
    """ parse a sequence of data in an itearable of line location and data
    """
    start = 0
    for byte_line in byte_stream:
        end = byte_stream.tell()
        line = byte_line.decode("utf8").strip()
        struct = json.loads(line) if line else None
        if line and isinstance(struct, (dict, list)):
            data = LocationData(start, end, struct)
            yield data
        start = end



def parse_stdin(str_stream: Iterable[str]) -> Iterable[LocationData]:
    """ parse a sequence of data in an itearable of line location and data
    """
    for line in str_stream:
        struct = json.loads(line) if line else None
        if line and isinstance(struct, (dict, list)):
            data = LocationData(-1, -1, struct)
            yield data



def group(structs: Iterable[LocationData]) -> TableType:
    """take a sequence of line data and group them in tables
    """
    # utility functions for type testing later on
    instance_of = lambda types, obj: isinstance(obj.data, types)
    is_obj_or_arr = partial(instance_of, (dict, list))
    is_arr = partial(instance_of, list)
    # keep only objects and arrays
    good_elements = filter(is_obj_or_arr, structs)
    # remove the array that are at the beginning
    good_structs = it.dropwhile(is_arr, good_elements)
    # group together all the objects and all the arrays
    grouped = it.groupby(good_structs, lambda obj: type(obj.data))
    # pair the header and the data and return them in pairs
    header = LocationData(-1, -1, []) # empty string
    for kind, seq in grouped:
        # don't need to initialize the header,
        # I've dropped all the other objects before
        if issubclass(kind, dict):
            # get the last element,
            # i.e. drop all the header with no data that follow them
            *_, header = seq
        else:
            yield header, seq


if __name__ == "not_active_anymore":
    FAKE_STRING = """
        {"name": "types", "columns": ["table", "column", "type"]}
            ["names", "name", "string"]
            ["names", "age", "number"]
            ["jobs", "name", "string"]
            ["jobs", "job", "string"]

        {"name": "people", "columns": ["name", "age"]}
            ["john",    25]
            ["karen",   32]
            ["jeanine", 54]

        {"name": "jobs", "columns": ["name", "job"]}
            ["karen",   "doctor"]
            ["john",    "plumber"]
            ["jeanine", "teacher"]
        """

# %%

def main_keep(args):
    regex = args.regex
    regex = re.compile(regex)

    with printable(args.output) as output:
        for _header, _data in group(parse(datastream(args.input))):
            tablename = _header.data['name']
            if regex.match(tablename):
                for _line in _data:
                    print(_line.data.rstrip(), file=output)

def main_no_command(args):
    parser.parse_args(["--help"])
    sys.exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser_subparsers = parser.add_subparsers(
        dest="command",
        title="subcommands",
        description='valid subcommands',
        )

    # keep only the tables whose name match a regex
    subparser = parser_subparsers.add_parser(
        'grep',
        help="select a subset of tables and lines from a jmt file",
        )
    # query to keep or remove tables and lines
    # first it decides which tables to keep, then which to remove of the kept ones
    # then applies the same logic to the lines of each table
    subparser.add_argument( "--keep-line", type=str,)
    subparser.add_argument( "--drop-line", type=str,)
    subparser.add_argument( "--drop-table", type=str,)
    subparser.add_argument( "--keep-table", type=str,)
    # aggiungo argomenti di default per input ed output
    add_stream_input(subparser)
    add_stream_output(subparser)


    subcommands_functions = {
        "grep": main_grep,
        }
    # uses the command set in the redirect dictionary
    # if not provided, uses the default `main_no_command`
    args = parser.parse_args()
    command_function = subcommands_functions.get(args.command, main_no_command)
    command_function(args)

# %% end
