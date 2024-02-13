import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 



class TestHeader(PysparkBaseTest):

    #
    # Test returning the edi deliminter from the message header 
    # 
    def test_delimiter(self):
        pass #TODO 

class TestSegment(PysparkBaseTest):

    #
    # Test 
    #
    def test_segment_length(self):
        pass #TODO
