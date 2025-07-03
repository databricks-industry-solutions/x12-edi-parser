import unittest, pickle, tempfile, os, glob
from ember.edi import EDI, Segment
from ember.format import *
from ember.transaction import Transaction
from ember.functional import FunctionalGroup
import itertools


class TestPickle(unittest.TestCase):
    """Test cases for pickle functionality using equality statements"""

    def setUp(self):
        """Set up test data from sample files"""
        # Load sample EDI files
        self.sample_files = glob.glob("sampledata/837/*.txt")
        self.edi_objects = [EDI(open(file, "rb").read().decode("utf-8"), strict_transactions=False) for file in self.sample_files]
        self.segment_objects = list(itertools.chain.from_iterable([edi.data for edi in self.edi_objects]))

    def test_segment_pickle_equality(self):
        """Test that Segment objects maintain equality after pickling"""
        for segment in self.segment_objects:
            # Serialize
            pickled_data = pickle.dumps(segment)
            # Deserialize
            restored_segment = pickle.loads(pickled_data)

            # Test equality
            self.assertEqual(segment, restored_segment, 
                           f"Segment equality failed for segment: {segment.data[:50]}...")

    def test_edi_pickle_equality(self):
        """Test that EDI objects maintain equality after pickling"""
        for edi_obj in self.edi_objects:
            # Serialize
            pickled_data = pickle.dumps(edi_obj)
            
            # Deserialize
            restored_edi = pickle.loads(pickled_data)
            
            # Test equality
            self.assertEqual(edi_obj, restored_edi, 
                           f"EDI equality failed for file with {edi_obj.segment_count()} segments")

    def test_format_pickle_equality(self):
        """Test Format object serialization with equality verification"""
        # Test different format types
        format_objects = [
            AnsiX12Delim(AnsiX12Delim.SEGMENT_DELIM, AnsiX12Delim.ELEMENT_DELIM, AnsiX12Delim.SUB_DELIM),
            EDIFactDelim(EDIFactDelim.SEGMENT_DELIM, EDIFactDelim.ELEMENT_DELIM, EDIFactDelim.SUB_DELIM),
            Format("~", "*", ":"),
            Format("'", "+", ":"),
            Format("|", "^", "~")
        ]
        
        for format_obj in format_objects:
            # Serialize
            pickled_data = pickle.dumps(format_obj)
            
            # Deserialize
            restored_format = pickle.loads(pickled_data)
            
            # Test equality
            self.assertEqual(format_obj, restored_format, 
                           f"Format equality failed for {format_obj.__class__.__name__}")
            self.assertFalse(format_obj != restored_format)
            
            # Test delimiter values are preserved
            self.assertEqual(restored_format.SEGMENT_DELIM, format_obj.SEGMENT_DELIM)
            self.assertEqual(restored_format.ELEMENT_DELIM, format_obj.ELEMENT_DELIM)
            self.assertEqual(restored_format.SUB_DELIM, format_obj.SUB_DELIM)

    def test_transaction_pickle_equality(self):
        """Test Transaction object serialization with equality verification"""
        # Create transaction objects from EDI data
        transaction_objects = []
        
        for edi_obj in self.edi_objects:
            # Get functional groups and their transactions
            for fg in edi_obj.functional_segments():
                for transaction in fg.transaction_segments():
                    transaction_objects.append(transaction)
        
        # If no transactions found, create a simple test transaction
        if not transaction_objects:
            # Create a simple test transaction
            test_segments = [
                Segment("ST*837*0001*005010X222A1"),
                Segment("BHT*0019*00*0123456789*20230101*1200*CH"),
                Segment("SE*3*0001")
            ]
            test_transaction = Transaction(test_segments, delim_cls=AnsiX12Delim, transaction_type="837")
            transaction_objects.append(test_transaction)
        
        for transaction in transaction_objects:
            # Serialize
            pickled_data = pickle.dumps(transaction)
            
            # Deserialize
            restored_transaction = pickle.loads(pickled_data)
            
            # Test equality
            self.assertEqual(transaction, restored_transaction, 
                           f"Transaction equality failed for transaction type: {transaction.transaction_type}")
            
            # Test properties are preserved
            self.assertEqual(restored_transaction.transaction_type, transaction.transaction_type)
            self.assertEqual(len(restored_transaction.data), len(transaction.data))
            
            # Test format class is preserved
            self.assertEqual(restored_transaction.format_cls.SEGMENT_DELIM, transaction.format_cls.SEGMENT_DELIM)
            self.assertEqual(restored_transaction.format_cls.ELEMENT_DELIM, transaction.format_cls.ELEMENT_DELIM)
            self.assertEqual(restored_transaction.format_cls.SUB_DELIM, transaction.format_cls.SUB_DELIM)
            
            # Test class type is preserved
            self.assertEqual(restored_transaction.__class__.__name__, transaction.__class__.__name__)
            
            # Test segment data integrity
            for i, segment in enumerate(transaction.data):
                self.assertEqual(restored_transaction.data[i], segment)

    def test_functional_group_pickle_equality(self):
        """Test FunctionalGroup object serialization with equality verification"""
        # Create functional group objects from EDI data
        functional_group_objects = []
        
        for edi_obj in self.edi_objects:
            # Get functional groups
            fgs = edi_obj.functional_segments()
            functional_group_objects.extend(fgs)
        
        # If no functional groups found, create a simple test functional group
        if not functional_group_objects:
            # Create a simple test functional group
            test_segments = [
                Segment("GS*HC*SENDER*RECEIVER*20230101*1200*1*X*005010X222A1"),
                Segment("ST*837*0001*005010X222A1"),
                Segment("BHT*0019*00*0123456789*20230101*1200*CH"),
                Segment("SE*3*0001"),
                Segment("GE*1*1")
            ]
            test_fg = FunctionalGroup(test_segments, delim_cls=AnsiX12Delim, strict_transactions=False)
            functional_group_objects.append(test_fg)
        
        for fg in functional_group_objects:
            # Serialize
            pickled_data = pickle.dumps(fg)
            
            # Deserialize
            restored_fg = pickle.loads(pickled_data)
            
            # Test equality
            self.assertEqual(fg, restored_fg, 
                           f"FunctionalGroup equality failed for transaction type: {fg.transaction_type}")
            
            # Test properties are preserved
            self.assertEqual(restored_fg.transaction_type, fg.transaction_type)
            self.assertEqual(restored_fg.standard_version, fg.standard_version)
            self.assertEqual(restored_fg.control_number, fg.control_number)
            self.assertEqual(restored_fg.date, fg.date)
            self.assertEqual(restored_fg.time, fg.time)
            self.assertEqual(restored_fg.sender, fg.sender)
            self.assertEqual(restored_fg.receiver, fg.receiver)
            self.assertEqual(len(restored_fg.data), len(fg.data))
            
            # Test format class is preserved
            self.assertEqual(restored_fg.format_cls.SEGMENT_DELIM, fg.format_cls.SEGMENT_DELIM)
            self.assertEqual(restored_fg.format_cls.ELEMENT_DELIM, fg.format_cls.ELEMENT_DELIM)
            self.assertEqual(restored_fg.format_cls.SUB_DELIM, fg.format_cls.SUB_DELIM)
            
            # Test class type is preserved
            self.assertEqual(restored_fg.__class__.__name__, fg.__class__.__name__)
            
            # Test segment data integrity
            for i, segment in enumerate(fg.data):
                self.assertEqual(restored_fg.data[i], segment)
            
            # Test functional group metadata integrity
            self.assertEqual(restored_fg.fg, fg.fg)
            
            # Test transaction segments functionality
            original_transactions = fg.transaction_segments()
            restored_transactions = restored_fg.transaction_segments()
            self.assertEqual(len(original_transactions), len(restored_transactions))
            
            # Test strict transactions flag
            self.assertEqual(restored_fg._strict_transactions, fg._strict_transactions)


if __name__ == '__main__':
    unittest.main() 