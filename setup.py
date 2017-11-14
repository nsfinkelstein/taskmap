from setuptools import setup, find_packages

setup(
    name='dgraph',
    version='0.0.6',
    description='Simple dependency graph for python functions',
    url='https://github.com/n-s-f/dgraph',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['bunch', 'multiprocess'],
)
