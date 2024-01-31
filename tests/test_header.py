import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 


class PysparkBaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestHeader").config("spark.driver.memory", "4g").getOrCreate()
        
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

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
