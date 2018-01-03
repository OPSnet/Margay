Margay
======

Margay is a BitTorrent tracker written in Python (though intended to be compiled with Cython) for the Gazelle project.
The first release is aimed to be 1-to-1 translation of Ocelot into Python to be then stress tested to see a comparison
of efficiency between the two projects (with Margay then having the edge in developer productivity).

Dependencies
------------
* Python 3.6
* `aiohttp <https://aiohttp.readthedocs.io/en/stable/>`_
* `bencode.py <https://pypi.python.org/pypi/bencode.py>`_
* `mysqlclient <https://pypi.python.org/pypi/mysqlclient>`_
* `requests <http://docs.python-requests.org/en/master/>`_

Installation
------------
After cloning or downloading this repository, navigate to it and run::

  python setup.py install


Usage
-----
Running margay is easy from this repo::

    usage: runner.py [-h] [-v] [-d] [-c [CONFIG]] [-V]

    Python BitTorrent tracker

    optional arguments:
      -h, --help            show this help message and exit
      -v, --verbose         Be more verbose in the output
      -d, --daemonize       Run tracker as daemon
      -c [CONFIG], --config [CONFIG]
      -V, --version         show program's version number and exit

Gazelle
^^^^^^^
After installing Gazelle, you should be able to point Margay towards that database and things should just work.
Management of torrents, users, tokens, and the whitelist can all be done via the Gazelle site and it will be
communicated to Margay. However, you must make sure that the Gazelle configuration (`classes/config.php`) is configured
to point to where Margay is running and that both Margay and Gazelle have the same passwords configured in their
respective configurations.

Roadmap:
--------
1. Develop a "Leopardus Tracker Tester" which would test Ocelot/Margay for compliance with each other as well as benchmark
2. Use the benchmarks to determine if it's worth developing this further
3. Investigate dropping aiohttp for `japronto <https://github.com/squeaky-pl/japronto>`_ for potential speed-up

See Also:
---------
* [pybtracker](https://github.com/elektito/pybtracker)
* [Ocelot](https://github.com/ApolloRIP/Ocelot)