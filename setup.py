from setuptools import setup, find_packages

setup(
    name='taskmap',
    version='0.0.7',
    description='Dependency graph with parallel asyncronous task runner',
    url='https://github.com/n-s-f/taskmap',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['multiprocess', 'multiprocessing-logging'],
)
