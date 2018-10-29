import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

class PyTest(TestCommand):
    user_options = [("pytest-args=", "a", "Arguments to pass to pytest")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ""

    def run_tests(self):
        import shlex
        import pytest
        errno = pytest.main(shlex.split(self.pytest_args))
        sys.exit(errno)
        

VERSION="0.1.6.dev1"


setup(
    name="aaew_couch",
    version=VERSION,
    packages=find_packages(),
    install_requires=[
        'CouchDB',
        ],
    extras_require={
        'progress_bar': ['tqdm'],
        },
    setup_requires=[
        'pytest-runner',
        ],
    test_suite='tests',
    tests_require=[
        'pytest',
        ],
    cmdclass={
        'test': PyTest,
        }
)


