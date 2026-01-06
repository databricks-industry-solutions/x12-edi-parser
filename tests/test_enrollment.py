import unittest, os, json
from .test_spark_base import *
from ember.hls.healthcare import HealthcareManager as hm
from ember.edi import EDI, Segment
from ember.hls.enrollment import MemberEnrollment

# NOTE: For standalone INS segment count validation tests that don't require PySpark,
# see test_enrollment_ins_count.py


class TestEnrollment(PysparkBaseTest):
    
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Load test data from 834 files
        cls.test_data_1 = EDI(open("sampledata/834/834_test.txt", "rb").read().decode("utf-8"), strict_transactions=False)
        cls.test_data_2 = EDI(open("sampledata/834/EDI_834.txt", "rb").read().decode("utf-8"), strict_transactions=False)
        

    def test_ins_segment_count_matches_member_count_834_test(self):
        """Test that INS segment count matches the number of enrollment records for 834_test.txt"""
        # Count INS segments manually
        ins_count = len([seg for seg in self.test_data_1.data if seg._name == "INS"])
        
        # Get enrollment records from HealthcareManager
        enrollments = hm.from_edi(self.test_data_1)
        
        # Assert counts match
        self.assertEqual(ins_count, len(enrollments), 
                        f"INS segment count ({ins_count}) should match enrollment record count ({len(enrollments)}) for 834_test.txt")
        
    def test_ins_segment_count_matches_member_count_edi_834(self):
        """Test that INS segment count matches the number of enrollment records for EDI_834.txt"""
        # Count INS segments manually
        ins_count = len([seg for seg in self.test_data_2.data if seg._name == "INS"])
        
        # Get enrollment records from HealthcareManager
        enrollments = hm.from_edi(self.test_data_2)
        
        # Assert counts match
        self.assertEqual(ins_count, len(enrollments),
                        f"INS segment count ({ins_count}) should match enrollment record count ({len(enrollments)}) for EDI_834.txt")

    def test_ins_segment_count_using_segments_by_name(self):
        """Test that INS count from segments_by_name matches from_edi result count"""
        for test_data, file_name in [(self.test_data_1, "834_test.txt"),
                                      (self.test_data_2, "EDI_834.txt")]:
            # Count using segments_by_name method
            ins_segments = test_data.segments_by_name("INS")
            
            # Get enrollment records
            enrollments = hm.from_edi(test_data)
            
            # Assert counts match
            self.assertEqual(len(ins_segments), len(enrollments),
                           f"segments_by_name('INS') count ({len(ins_segments)}) should match "
                           f"enrollment count ({len(enrollments)}) for {file_name}")

    def test_each_enrollment_has_unique_segments(self):
        """Test that each enrollment record gets the correct segment range"""
        enrollments = hm.from_edi(self.test_data_1)
        
        # Get all INS segment indices
        ins_indices = [i for i, seg in enumerate(self.test_data_1.data) if seg._name == "INS"]
        
        # Verify each enrollment has at least one INS segment in its member_detail_loop
        for i, enrollment in enumerate(enrollments):
            # Each enrollment should have member_detail_loop
            self.assertTrue(hasattr(enrollment, 'member_detail_loop'),
                          f"Enrollment {i} should have member_detail_loop")
            
            # The member_detail_loop should contain at least one INS segment
            ins_in_loop = [seg for seg in enrollment.member_detail_loop if seg._name == "INS"]
            self.assertEqual(len(ins_in_loop), 1,
                           f"Enrollment {i} should have exactly 1 INS segment in its member_detail_loop")
            
            # Verify it's the correct INS segment (by index order)
            if i < len(ins_indices):
                # The first segment in member_detail_loop should be at the expected INS index
                first_seg = enrollment.member_detail_loop[0]
                self.assertEqual(first_seg._name, "INS",
                               f"First segment in enrollment {i} should be INS")

    def test_no_overlapping_segments_between_enrollments(self):
        """Test that segment ranges don't overlap between enrollments"""
        enrollments = hm.from_edi(self.test_data_1)
        
        # Build a set of segment data strings from all enrollments
        all_segment_positions = []
        for i, enrollment in enumerate(enrollments):
            for seg in enrollment.member_detail_loop:
                # Use segment data as unique identifier
                all_segment_positions.append((i, seg.data))
        
        # Check that if we group by segment data, each appears in only one enrollment
        from collections import defaultdict
        segment_to_enrollments = defaultdict(list)
        for enroll_idx, seg_data in all_segment_positions:
            segment_to_enrollments[seg_data].append(enroll_idx)
        
        # Verify no segment appears in multiple enrollments
        for seg_data, enroll_indices in segment_to_enrollments.items():
            self.assertEqual(len(set(enroll_indices)), 1,
                           f"Segment '{seg_data[:50]}...' should only appear in one enrollment, "
                           f"but found in enrollments: {enroll_indices}")


if __name__ == '__main__':
    unittest.main()
