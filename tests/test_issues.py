from .test_spark_base import *
from .test_pyspark import *
from databricksx12.hls import *
from databricksx12 import *
import unittest, re
from os import listdir
from os.path import isfile, join

class TestIssues(PysparkBaseTest):

    #Pulling delimiters from ISA 
    def test_issue21(self):
        files = ['sampledata/837/' + f for f in listdir('sampledata/837') if isfile(join('sampledata/837', f))]
        assert(set(map(lambda f:
                 EDI.extract_delim(open(f, "rb").read().decode("utf-8")) == AnsiX12Delim
            , files)) == {True})

    
