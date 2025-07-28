import unittest, re
from .test_spark_base import *
from ember.edi import *
import itertools

class TestEDI(PysparkBaseTest):

    x = EDI(open("sampledata/837/CC_837I_EDI.txt", "rb").read().decode("utf-8"))
    y = EDI(open("sampledata/837/CC_837P_EDI.txt", "rb").read().decode("utf-8"))
    z = EDI(open("sampledata/837/CHPW_Claimdata.txt", "rb").read().decode("utf-8"))

    def test_no_isa(self):
        x = EDI(open("sampledata/malformed_files/CC_837I_EDI.txt", "rb").read().decode("utf-8"), strict_transactions = False)
        assert(x.to_json().get('EDI.control_number') == "")
        
    
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
        assert(TestEDI.x.segments_by_position(0,1)[0]._name == "ISA")
        assert(TestEDI.y.segments_by_position(0,1)[0]._name == "ISA")
        assert(len(TestEDI.x.segments_by_position(0,1)) == 1)
        assert(len(TestEDI.x.segments_by_position(0,100)) == 66)
        assert(TestEDI.x.segments_by_position(15, 20)[0]._name == "NM1")

    #SE01 should contain a value of total segments including ST segment start
    def test_se01_failure(self):
        assert ( TestEDI.x._valid_se01() )
        e = '~'.join([x.data for x in TestEDI.x.data[:10] + TestEDI.x.data[11:]])
        self.assertRaises(Exception, EDI, e)
        assert( len(EDI(e, strict_transactions = False).data) == 64)

    def test_functional_groups(self):
        assert(len(TestEDI.z.functional_segments()) == 1)
        assert(len(TestEDI.z.functional_segments()[0].data) == 173)
        assert(TestEDI.z.num_transactions() == 5)
        assert(TestEDI.z.num_functional_groups() == 1)
        assert(TestEDI.z.functional_segments()[0].data[0].element(0) == "GS" and TestEDI.z.functional_segments()[0].data[172].element(0) == "GE")
        assert(len([a.element(0) for a in TestEDI.z.functional_segments()[0].data if a.element(0) == "ST"]) == TestEDI.z.num_transactions())

    def test_functional_group_locations(self):
        assert(TestEDI.y.segments_by_name_index("GS")[0][0] == 1)
        assert(TestEDI.y.segments_by_name_index("GE")[0][0] == 32)
        assert(TestEDI.y._functional_group_locations()[0] == (1,32))
        assert(TestEDI.y.functional_segments()[0].data[0].element(0) == 'GS')
        assert(len(TestEDI.y.functional_segments()[0].data) == 32)
        assert(TestEDI.y.functional_segments()[0].data[31].element(0) == 'GE')


    def test_locations_non_strict_setting(self):
        z = EDI(open("sampledata/837/CHPW_Claimdata.txt", "rb").read().decode("utf-8"), strict_transactions=False)
        assert(set([z.data[s]._name == 'ST' and z.data[e]._name == 'SE' for s,e in TestEDI.z._transaction_locations()]) == {True})


    def test_functional_group_locaitons(self):
        assert(set([TestEDI.z.data[s]._name == 'GS' and TestEDI.z.data[e]._name == 'GE' for s,e in TestEDI.z._functional_group_locations()]) == {True})
        assert(set([TestEDI.z.data[s]._name == 'ST' and TestEDI.z.data[e]._name == 'SE' for s,e in TestEDI.z._transaction_locations()]) == {True})

        assert(set([TestEDI.y.data[s]._name == 'GS' and TestEDI.y.data[e]._name == 'GE' for s,e in TestEDI.y._functional_group_locations()]) == {True})
        assert(set([TestEDI.y.data[s]._name == 'ST' and TestEDI.y.data[e]._name == 'SE' for s,e in TestEDI.y._transaction_locations()]) == {True})
 
        assert(set([TestEDI.x.data[s]._name == 'GS' and TestEDI.x.data[e]._name == 'GE' for s,e in TestEDI.x._functional_group_locations()]) == {True})
        assert(set([TestEDI.x.data[s]._name == 'ST' and TestEDI.x.data[e]._name == 'SE' for s,e in TestEDI.x._transaction_locations()]) == {True})

    def test_transaction_locations_in_functional_group(self):
        zfg = TestEDI.z.functional_segments()
        yfg = TestEDI.y.functional_segments()
        xfg = TestEDI.x.functional_segments()

        #each fg starts and ends with GS/GE
        assert(set([z.data[0]._name == 'GS' and z.data[-1]._name == 'GE' for z in zfg]) == {True})
        assert(set([y.data[0]._name == 'GS' and y.data[-1]._name == 'GE' for y in yfg]) == {True})
        assert(set([x.data[0]._name == 'GS' and x.data[-1]._name == 'GE' for x in xfg]) == {True})

        #each transaction location finder starts and ends with ST/SE
        assert([set([z.data[s]._name == 'ST' and  z.data[e]._name == 'SE' for s, e in z._transaction_locations()]) for z in zfg] == [{True}])
        assert([set([y.data[s]._name == 'ST' and  y.data[e]._name == 'SE' for s, e in y._transaction_locations()]) for y in yfg] == [{True}])
        assert([set([x.data[s]._name == 'ST' and  x.data[e]._name == 'SE' for s, e in x._transaction_locations()]) for x in yfg] == [{True}])

        #each transaction build staarts and ends with ST/SE
        zfgt = list(itertools.chain(*[z.transaction_segments() for z in zfg]))
        yfgt = list(itertools.chain(*[y.transaction_segments() for y in yfg]))
        xfgt = list(itertools.chain(*[x.transaction_segments() for x in xfg]))

        assert(set([z.data[0]._name == 'ST' and z.data[-1]._name == 'SE' for z in zfgt]) == {True})
        assert(set([y.data[0]._name == 'ST' and y.data[-1]._name == 'SE' for y in zfgt]) == {True})
        assert(set([x.data[0]._name == 'ST' and x.data[-1]._name == 'SE' for x in zfgt]) == {True})


if __name__ == '__main__':
    unittest.main()
