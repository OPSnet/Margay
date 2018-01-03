from setuptools import setup
from margay import __author__, __version__

setup(
    name='Margay',
    author=__author__,
    version=__version__,
    url='https://github.com/ApolloRIP/Margay',
    description='A Python BitTorrent Server',
    long_description=open('README.rst').read(),
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Cython',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License'
    ],
    install_requires=[
        'aiohttp',
        'bencode.py',
        'mysqlclient',
        'requests'
    ]
)
