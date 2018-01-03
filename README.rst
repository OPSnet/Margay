Margay
======

Margay is a BitTorrent tracker written in Python (though intended to be compiled with Cython) for the Gazelle project.
The first release is aimed to be 1-to-1 translation of Ocelot into Python to be then stress tested to see a comparison
of efficiency between the two projects (with Margay then having the edge in developer productivity).

## Dependencies
Python 3.5+
mysqlclient

## Installation
```
python setup.py install
```

### Standalone
While its generally recommended that you run Margay in conjuction with a Gazelle instance, it's possible to run this
in standalone fashion.

### Gazelle

## Usage
```
usage: runner.py [-h] [-v] [-d] [-c [CONFIG]] [-V]

Python BitTorrent tracker

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         Be more verbose in the output
  -d, --daemonize       Run tracker as daemon
  -c [CONFIG], --config [CONFIG]
  -V, --version         show program's version number and exit
```

## See Also:
* [pybtracker](https://github.com/elektito/pybtracker)
* [Ocelot](https://github.com/ApolloRIP/Ocelot)