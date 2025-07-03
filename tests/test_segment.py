import unittest, re
from .test_spark_base import *
from ember.edi import *

class TestSegment(PysparkBaseTest):

    data = open("sampledata/837/CC_837I_EDI.txt", "rb").read().decode("utf-8")
    segments = [Segment(x) for x in re.split(r'~[\r]', data)][:-1]
    
    #
    # Test Segments 
    #
    def test_segment_length(self):
        assert(len(TestSegment.segments) == 66)
        assert( set([s.segment_len() == len(s.data.split("*")) for s in TestSegment.segments]) == {True} )
        
    def test_sub_element_length(self):
        assert(len(TestSegment.segments) == 66)
        assert( set([s.sub_element_len(0) == len(s.data.split(s.format_cls.ELEMENT_DELIM)[0].split(":")) for s in TestSegment.segments]) == {True} )
        assert( set([s.sub_element_len(1) == len(s.data.split(s.format_cls.ELEMENT_DELIM)[1].split(":")) for s in TestSegment.segments]) == {True} )
        

    def test_get_elements(self):
        assert ( TestSegment.segments[0].element(0) == TestSegment.segments[0].element(0, 0) == TestSegment.segments[0].element(0, -1) == 'ISA' )
        assert ( TestSegment.segments[0].element(0, 1) == TestSegment.segments[0].element(0, 2) == '' )
        assert ( TestSegment.segments[0].element(0, 1, dne='foobar') == TestSegment.segments[0].element(0, 2, dne='foobar') == 'foobar' )
        assert ( TestSegment.segments[22].element(5) == '11:A:1' )
        assert ( TestSegment.segments[22].element(5, 0) + ":" + TestSegment.segments[22].element(5, 1) + ":" +  TestSegment.segments[22].element(5, 2) == '11:A:1' )
        assert ( TestSegment.segments[22].element(5, 3) == "" )

if __name__ == '__main__':
    unittest.main()
