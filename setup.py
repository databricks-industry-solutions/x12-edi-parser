from setuptools import setup
from io import open
from os import path
import sys

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()

setup(
    name="databricksx12",
    version="0.0.6",
    # python_requires='>=3.9.*',
    python_requires='>=3.9',
    author="Aaron Zavora, Raven Mukherjee",
    author_email="aaron.zavora@databricks.com",
    description= "Parser for handling x12 EDI transactions in Spark",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://github.com/databricks-industry-solutions/x12-edi-parser",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    packages=['databricksx12', 'databricksx12.hls', 'ember', 'ember.hls'],
    package_dir={
        'databricksx12': 'databricksx12',
        'ember': 'src/databricksx12' 
    },
    py_modules=['databricksx12', 'ember'],
    extras_require={
        'test': [
            'pyspark>=3.4.0',
            'pytest>=7.0.0',
            'pytest-spark>=0.6.0'
        ]
    }
)
