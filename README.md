# JSON Search

## Overview

This utility `jsonsearch` allows the user to load JSON files along with relationship information provided (if any) into memory and perform searches and cross referenced searches across JSON documents and pretty prints the results/findings.

## Build Instructions

The following software is required to build -

- GNU Make
- Go (1.15+)

Make sure to set `GOBIN` environment variable and set that to your `PATH` environment variable.

After the pre-requisites above are complete -

```
git clone https://github.com/gusaki/jsonsearch.git
cd jsonsearch
make
```

### Other Makefile targets

```
make all      # will build, test and install the binary
make test     # runs the tests
make coverage # runs coverage and displays coverage in browser
make bench.   # runs benchmark tests
```

## Execution Instructions

The utility `jsonsearch` runs in command line mode and interactive mode. Refer to the `help` instructions of the tool. 

Typing `jsonsearch -help` displays all the available options -

```
-dbfiles value
        Comma separated list of filenames/filepaths
-indexby value
        Comma separated list of index keys. In the form of <filename.json_key>.
                Example: organizations._id,tickets.id
-interactive
        Run in interactive mode
-keypath string
        Dot separated path to the JSON key
-relationships value
        Comma separated list of relationships
        with each relationship delimited with a colon.
                Example: organizations._id:tickets.organization_id,users.organization_id:organizations._id
-searchdb string
        Name of database to search
-searchvalue string
        Search value

Usage examples:
Command line mode:
        jsonsearch -dbfiles /home/u/org.json,/home/u/tickets.json,/home/u/users.json \
        -indexby org._id:tickets.id -relationships org._id:users.org_id \
        -searchdb org -keypath _id -value 101

Interactive Mode (also requires some initialization parameters)
        jsonsearch -dbfiles /home/u/org.json,/home/u/tickets.json,/home/u/users.json \
        -indexby org._id:tickets.id -relationships org._id:users.org_id \
	-interactive
```
