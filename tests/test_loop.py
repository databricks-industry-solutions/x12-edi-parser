from test_spark_base import *
from databricksx12.hls.loop import *
import unittest, re


class TestLoop(PysparkBaseTest):

    data = open("sampledata/837/837p.txt", "rb").read().decode("utf-8")
    loop = Loop(data)
    
    #
    # Test Loop base info
    #
    def test_loop_hierarchy_build(self):
        assert (set(TestLoop.loop.loop_hierarchy.keys()) == set({'1','2','3'}))
        assert (TestLoop.loop.loop_hierarchy.get('1')['start_idx'] == 7)
        assert (TestLoop.loop.loop_hierarchy.get('2')['start_idx'] == 12)
        assert (TestLoop.loop.loop_hierarchy.get('3')['start_idx'] == 27)
        assert (TestLoop.loop.loop_hierarchy.get('1')['end_idx'] == 12)
        assert (TestLoop.loop.loop_hierarchy.get('2')['end_idx'] == 27)
        assert (TestLoop.loop.loop_hierarchy.get('3')['end_idx'] == 45)
        assert ([x.get('hl_code') for x in list(TestLoop.loop.loop_hierarchy.values())] == ['20','22','22'])
        assert ([x.get('child_code') for x in list(TestLoop.loop.loop_hierarchy.values())] == ['1','0','0'])
        

    #
    # Test traversing hierarchy 
    #
    def test_loop_hierarchy(self):
        clms = TestLoop.loop.segments_by_name_index("CLM")
        assert (clms[0][0] == 22)
        assert (clms[1][0] == 37)
        
        assert (TestLoop.loop.find_hl_codes(22, '20') == TestLoop.loop.find_hl_codes(37, '20'))
        assert (TestLoop.loop.find_hl_codes(22, '22') !=  TestLoop.loop.find_hl_codes(37, '22'))

        assert  (TestLoop.loop.find_hl_codes(22, '20')['start_idx'] == 7)
        assert  (TestLoop.loop.find_hl_codes(22, '22')['start_idx'] == 12)
        assert  (TestLoop.loop.find_hl_codes(37, '22')['start_idx'] == 27)
        
if __name__ == '__main__':
    unittest.main()        
        

