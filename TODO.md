# TODO for the parser
## data format

general operations:

* [X] select subset of tables based on regex on the name
* [X] execute SQL query on the data
* [X] generate some example file
* [ ] apply jmespath filters on the data
* [ ] provide installation script with entry points

data formats:

* [X] export to and from Excel file
* [X] export to and from SQLlite file
* [X] export to and from plain JSONlines file
* [ ] export to and from CSV/TSV files
* [ ] export to and from HDF5 file

external libraries dependencies:

* [ ] remove dependency from pandas if possible
* [ ] make dependency from openpyxl necessary only to work with excel, not always



## data structures

* [ ] numpy array storage and retrieval
* [ ] binary data index build as a separate file


I think that the numpy data storage and retrieval should probably be a separate module compared to the basic parser.

## design notes

right now the objects without any following array are discarded as void tables.
Would it make sense to include them as forms of metadata?
It would be probably worth keeping them to simply represents empty tables, to avoid losing track that those tables should exists... but it might create problems for reversibility of format: for example it would be impossible to back generate from jsonlines when the file comes out as empty...

## JSONDL
it might be worth exploring a connection with the jsondl and various common data models.
