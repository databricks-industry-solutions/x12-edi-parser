import unittest, re
from test_spark_base import *
from databricksx12.edi import *

class TestEDI(PysparkBaseTest):

    x = EDI(open("sampledata/837/CC_837I_EDI.txt", "rb").read().decode("utf-8"))
    y = EDI(open("sampledata/837/CC_837P_EDI.txt", "rb").read().decode("utf-8"))
    #
    # Test counting total number of segments 
    # 
    def test_segment_count(self):
        assert ( TestEDI.x.segment_count() == 66)
        assert ( TestEDI.y.segment_count() == 34)

    def test_get_segment_by_name(self):
        assert ( len(TestEDI.x.segments_by_name("ISA")) == 1)
        assert ( len(TestEDI.y.segments_by_name("ISA")) == 1)
        assert ( len(TestEDI.x.segments_by_name("NM1")) == 6)
        assert ( len(TestEDI.y.segments_by_name("NM1")) == 5)
        assert ( set([ isinstance(type(x), type(Segment)) for x in TestEDI.x.segments_by_name("NM1") ]) == {True} )

    def test_segments_by_position(self):
        assert(TestEDI.x.segments_by_position(0,1)[0].segment_name() == "ISA")
        assert(TestEDI.y.segments_by_position(0,1)[0].segment_name() == "ISA")
        assert(len(TestEDI.x.segments_by_position(0,1)) == 1)
        assert(len(TestEDI.x.segments_by_position(0,100)) == 66)
        assert(TestEDI.x.segments_by_position(15, 20)[0].segment_name() == "NM1")

    
        
if __name__ == '__main__':
    unittest.main()
