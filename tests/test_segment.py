import unittest, re
from test_spark_base import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from databricksx12.edi import *

class TestSegment(PysparkBaseTest):

    """
    @classmethod
    def setUpClass(cls):
        cls.data837I = open("sampledata/837/CC_837I_EDI.txt", "r")
        cls.data837P = open("sampledata/837/Molina_Mock_UP_837P_File.txt", "r")
    """
    #
    # Test 
    #
    def test_segment_length(self):
        data = open("sampledata/837/CC_837I_EDI.txt", "rb").read().decode("utf-8")
        segments = [Segment(x) for x in re.split(r'~[\r]', data)]
        
    def test_sub_element_length(self):
        pass 

if __name__ == '__main__':
    unittest.main()
