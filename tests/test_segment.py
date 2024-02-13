import unittest
from test_spark_base import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 


class TestSegment(PysparkBaseTest):

    @classmethod
    def setUpClass(cls):
        data837I = open("sampledata/837/CC_837I_EDI.txt", "r")
    
    #
    # Test 
    #
    def test_segment_length(self):
        pass 

    def test_sub_element_length(self):
        pass 

if __name__ == '__main__':
    unittest.main()
