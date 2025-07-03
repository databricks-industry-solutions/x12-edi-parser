import unittest, os, json
from .test_spark_base import *
from ember.hls.healthcare import HealthcareManager as hm
from ember.edi import EDI, Segment
from ember.hls.enrollment import MemberEnrollment


class TestEnrollment(PysparkBaseTest):
    
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Load test data from 834 files
        #cls.test_data_1 = EDI(open("sampledata/834/834_test.txt", "rb").read().decode("utf-8"))
        cls.test_data_2 = EDI(open("sampledata/834/EDI_834.txt", "rb").read().decode("utf-8"))
        

    def test_enrollment_creation(self):
        """Test that MemberEnrollment objects can be created successfully"""
        assert(len(hm.from_edi(self.test_data_2)) == 1) 
        e2 = hm.from_edi(self.test_data_2)[0]
        assert(set(e2.to_json()) == set({'health_plan', 'enrollment_member'}))
        assert(e2.to_json()['health_plan'][0]['coverage_desc'] == 'Health')

    def test_enrollment_equality(self):
        part2a = hm.from_edi(self.test_data_2)[0].to_json() 
        part2b = hm.flatten_to_json(hm.flatten(self.test_data_2)[0])
        assert(part2b['enrollment_member'] == part2a['enrollment_member'] and part2b['health_plan'] == part2a['health_plan'])


if __name__ == '__main__':
    unittest.main()
