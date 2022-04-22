#!/usr/bin/env python
import sys
from typing import NamedTuple

class LocationData(NamedTuple):
    """Simple class to store the start and end of a line in a byte data source.

    it will be used to generate index for fast access.
    """
    start: int
    lenght: int
    data: str

def decorate_stream(stream):
    line_start_byte = 0
    for line in stream:
        num_bytes = len(line.encode("utf-8"))
        loc_data = LocationData(line_start_byte, num_bytes, line)
        line_start_byte += num_bytes
        yield loc_data

positions = []
with open("temporary_file.txt", encoding="utf-8", mode="w") as outfile:
    for loc_data in decorate_stream(sys.stdin):
        positions.append((loc_data.start, loc_data.lenght))
        print(loc_data.data, file=outfile, end='')
        print(repr(loc_data))

with open("temporary_file.txt", mode="rb") as infile:
    for start, n_bytes in positions:
        infile.seek(start)
        data = infile.read(n_bytes)
        line = data.decode("utf-8")
        print(line, end='')
