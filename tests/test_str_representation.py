import unittest
import json
from databricksx12.edi import EDI
from databricksx12.hls.healthcare import HealthcareManager

class TestStrRepresentation(unittest.TestCase):

    def test_claim837i_str(self):
        """Test the __str__ method for Claim837i"""
        with open("sampledata/837/CC_837I_EDI.txt", "rb") as f:
            edi_data = f.read().decode("utf-8")
        edi = EDI(edi_data)
        claims = HealthcareManager.from_edi(edi)
        for claim in claims:
            claim_str = str(claim)
            self.assertIsInstance(claim_str, str)
            self.assertTrue(len(claim_str) > 0)
            json.loads(claim_str) # Check if it's valid JSON

    def test_claim837p_str(self):
        """Test the __str__ method for Claim837p"""
        with open("sampledata/837/CC_837P_EDI.txt", "rb") as f:
            edi_data = f.read().decode("utf-8")
        edi = EDI(edi_data)
        claims = HealthcareManager.from_edi(edi)
        for claim in claims:
            claim_str = str(claim)
            self.assertIsInstance(claim_str, str)
            self.assertTrue(len(claim_str) > 0)
            json.loads(claim_str)

    def test_remittance_str(self):
        """Test the __str__ method for Remittance"""
        with open("sampledata/835/sample.txt", "rb") as f:
            edi_data = f.read().decode("utf-8")
        edi = EDI(edi_data, strict_transactions=False)
        claims = HealthcareManager.from_edi(edi)
        for claim in claims:
            claim_str = str(claim)
            self.assertIsInstance(claim_str, str)
            self.assertTrue(len(claim_str) > 0)
            json.loads(claim_str)

    def test_enrollment_str(self):
        """Test the __str__ method for MemberEnrollment"""
        with open("sampledata/834/EDI_834.txt", "rb") as f:
            edi_data = f.read().decode("utf-8")
        edi = EDI(edi_data)
        claims = HealthcareManager.from_edi(edi)
        for claim in claims:
            claim_str = str(claim)
            self.assertIsInstance(claim_str, str)
            self.assertTrue(len(claim_str) > 0)
            json.loads(claim_str)

if __name__ == '__main__':
    unittest.main()
