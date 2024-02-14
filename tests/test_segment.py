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
    # Test Segments 
    #
    def test_segment_length(self):
        data = open("sampledata/837/CC_837I_EDI.txt", "rb").read().decode("utf-8")
        segments = [Segment(x) for x in re.split(r'~[\r]', data)][:-1]
        assert(len(segments) == 66)
        assert( set([s.element_len() == len(s.data.split("*")) for s in segments]) == {True} )
        
    def test_sub_element_length(self):
        data = open("sampledata/837/CC_837I_EDI.txt", "rb").read().decode("utf-8")
        segments = [Segment(x) for x in re.split(r'~[\r]', data)][:-1]
        assert(len(segments) == 66)
        assert( set([s.sub_element_len() == len(s.data.split(":")) for s in segments]) == {True} )

    def test_get_elements(self):
        data = open("sampledata/837/CC_837I_EDI.txt", "rb").read().decode("utf-8")
        segments = [Segment(x) for x in re.split(r'~[\r]', data)][:-1]
        assert ( segments[0].get_element(0) == segments[0].get_element(0, 0) == segments[0].get_element(0, -1) == 'ISA' )
        assert ( segments[0].get_element(0, 1) == segments[0].get_element(0, 2) == 'na/dne' )
        assert ( segments[0].get_element(0, 1, dne='foobar') == segments[0].get_element(0, 2, dne='foobar') == 'foobar' )
        assert ( segments[22].get_element(5) == '11:A:1' )
        assert ( segments[22].get_element(5, 0) + ":" + segments[22].get_element(5, 1) + ":" +  segments[22].get_element(5, 2) == '11:A:1' )
        assert ( segments[22].get_element(5, 3) == "na/dne" )

if __name__ == '__main__':
    unittest.main()
