import re, functools
from collections import ChainMap
from databricksx12.format import *

#
# Base class for parsing EDI x12
#  provides base interface to functional groups and transactions 
#   https://justransform.com/edi-essentials/edi-structure/
#
class EDI():

    #
    # @param df - the Dataframe of EDI transactions
    # @param column_name - the column containing the UTF-8 edi data
    # @param delim_class - class that contains the delimiter information for parsing the EDI transactions
    #             - AnsiX12Delim is the default and most used
    def __init__(self, data, delim_cls = AnsiX12Delim):
        self.raw_data = data
        self.format_cls = delim_cls
        self.data = [Segment(x, self.format_cls) for x in data.split(self.format_cls.SEGMENT_DELIM)[:-1]]
        self.isa = (self.segments_by_name("ISA")[0] if len(self.segments_by_name("ISA")) > 0 else Segment.empty())
        self.sender_qualifier_id = self.isa.element(5) + self.isa.element(6)
        self.recipient_qualifier_id = self.isa.element(7) + self.isa.element(8)
        self.standard_version=  self.isa.element(12)
        self.date = self.isa.element(9)
        self.time = self.isa.element(10)
        self.control_number = self.isa.element(13)
        
    #
    # Returns total count of segments
    #
    def segment_count(self):
        return len(self.data)

    #
    # Returns all segments matching segment_name
    #
    def segments_by_name(self, segment_name, range_start=-1, range_end=None, data = None):
        if data is None:
            data = self.data
        return [x for i,x in enumerate(data) if x.segment_name() == segment_name and range_start <= i <= (range_end or len(data))]

    #
    # Returns a tuple of all segments matching segment_name and their index
    #
    def segments_by_name_index(self, segment_name, range_start=-1, range_end = None, data = None):
        if data is None:
            data = self.data
        return [(i,x) for i,x in enumerate(data) if x.segment_name() == segment_name and range_start <= i <= (range_end or len(data))]

    #
    # Return the first occurence of the specified index 
    #
    def index_of_segment(self, segments, segment_name, search_start_idx=0):
        try:
            return min([(i) for i,x in enumerate(segments) if x.segment_name() == segment_name and i >=search_start_idx])
        except:
            return -1 #not found

        
    #
    # @param position_start - integer, the first segment to include (inclusive) starting at 0
    # @param position_end - integer, the last segment to include (exclusive) starting at 0
    #
    # @returns - all segements between start and end positions
    #          - if end_position is beyond last segment, returns up until last segment 
    #
    def segments_by_position(self, position_start, position_end):
        return self.data[position_start:position_end]

    #
    # Returns the total number of transactions using trailer segment 
    #
    def num_transactions(self):
        return len(self.segments_by_name("SE"))

    #
    # Functional groups
    #
    def num_functional_groups(self):
        return len(self.segments_by_name("GE"))

    #
    # Maps a list of indexes [0,4,7] to a series of ranges -> [(0,4), (4,7)]
    #
    def _index_to_tuples(self, indexes):
        return list((zip(indexes, indexes[1:])))
    
    #
    # Return all segments associated with each funtional group
    # [ fg1[SEGMENT1 ... SEGMENTN], fg2[SEGMENT1... SEGMENTN] ... ] 
    # GE01 element contains how many transactions are included in the group
    #
    #  
    #
    def functional_segments(self):
        from databricksx12.functional import FunctionalGroup
        return [FunctionalGroup(self.segments_by_position(a-1,b+1), self.format_cls) for a,b in self._functional_group_locations()]
                        

    def _functional_group_locations(self):
        return list(map(self._functional_segments_trx_list, [(i, int(y.element(1))) for i, y in self.segments_by_name_index("GE")]))

    #
    # Find all locations of a transaction
    #
    def _transaction_locations(self):
        return [(i - int(x.element(1))+1,i+1) for i,x in  self.segments_by_name_index("SE")]
    
    #
    # Fold left, given a
    #   @param functional_group = tuple (i,x)
    #   @param where i = start location in file of trailer segment
    #   @param where x = number of transactions in the functional group
    #
    #   @return a tuple of the start/end locations of the transactions
    #
    def _functional_segments_trx_list(self, functional_group):
        l = ()
        f = lambda trxs, x, fg: x if len(trxs) <= fg[1] and x[1] <= fg[0] else None
        return functools.reduce(lambda a,b: (min(a[0], b[0]), max(a[1], b[1])),
                    filter(
                        lambda y: y is not None, [l := f(l, x, functional_group) for x in self._transaction_locations()]
                    )
                )

    def to_json(self, exclude=["data", "raw_data", "isa"]):
        return {str(self.__class__.__name__ + "." + attr): getattr(self, attr) for attr in dir(self) if not callable(getattr(self, attr)) and not attr.startswith("__") and attr not in exclude}

    """
     Convert entire dataset into consumable row/column format
        Preserves the following information:
          *Segment names 
          *Row numbers for hierarchy
          *Row length for easy query access
          *Row data with split functionality to easily access row members 
    """
    def toRows(self):
        return [{"segment_name": x.segment_name()
                 ,"segment_length": x.segment_len()
                 ,"row_number": i
                 ,"row_data": x.data
                 ,"segment_element_delim_char": x.format_cls.ELEMENT_DELIM
                 ,"segment_subelement_delim_char": x.format_cls.SUB_DELIM} for i,x in enumerate(self.data)] 

    #
    # @returns - header class object from EDI
    #
    def header(self):
        return self.data[0]



class Segment():

    #
    # data 
    #
    def __init__(self, data, delim_cls = AnsiX12Delim):
        self.data = data.lstrip("\r").lstrip("\n").lstrip("\r\n")
        self.format_cls = delim_cls

    #
    # @param element - numeric value of the field (starting at 0)
    # @param sub_element - get a sub-element value (-1 for all, 0 through N for sub number)
    # @param dne - "Does Not Exist" value to return if  the element or sub element requested exceeds what is there
    # @returns a single element or sub element string
    #
    def element(self, element, sub_element=-1, dne=""):
        try:
            return ( self.data.split(self.format_cls.ELEMENT_DELIM)[element]
                     if sub_element == -1 else
                     self.data.split(self.format_cls.ELEMENT_DELIM)[element].split(self.format_cls.SUB_DELIM)[sub_element]
                    )
        except:
            return dne

    #
    # @returns number of elements in a segment 
    #
    def segment_len(self):
        return len(self.data.split(self.format_cls.ELEMENT_DELIM))

    #
    # @returns the number of sub elements in a segment
    #
    def sub_element_len(self):
        return len(self.data.split(self.format_cls.SUB_DELIM))

    #
    # First element is the segment name
    #
    def segment_name(self):
        return self.data.split(self.format_cls.ELEMENT_DELIM)[0]

    #
    # Filter this segment for element/sub_element values
    #
    def filter(self, value, element, sub_element, dne=""):
        return self if value == self.get_element(element, sub_element, dne) else None

    @classmethod
    def empty(cls):
        return cls(data="")


#
# Manage relationship heirarchy within EDI
# 
class EDIManager():
    
    def __init__(self, edi):
        self.data = {"edi": edi,
                     "functional_groups": [
                         {"fg": fg,
                          "transactions": fg.transaction_segments()}  for fg in edi.functional_segments()
                     ]
                    }

    #
    # Summary of all x12 components present
    #
    def summary(self):
        return {
            "Number of Segments": self.data["edi"].segment_count(),
            "Number of Functional Groups": self.data["edi"].num_functional_groups(),
            "Number of Transactions": self.data["edi"].num_transactions(),
            "Transaction Count by Type": 
              [{fg["fg"].transaction_type: fg["fg"].num_transactions()} for fg in self.data["functional_groups"]]
        }
        
    #
    # utility function for extracting values from different parts of EDI
    #
    #  @returns a python dictionary representing metadata found in EDI/FunctionalGroup/Transaction classes
    #
    @staticmethod
    def class_metadata(cls_obj, exclude=['data', 'raw_data', 'isa']):
        return {str(cls_obj.__class__.__name__ + "." + attr): getattr(cls_obj, attr) for attr in dir(cls_obj) if not callable(getattr(cls_obj, attr)) and not attr.startswith("__") and attr not in exclude}

    #
    # Flatten data from EDI to represent 
    #
    @staticmethod
    def flatten(data = None):
        if type(data) == type([]):
            return [EDIManager.flatten(d) for d in data]
        elif type(data) == type({}): 
            return {
                **dict(ChainMap(*[EDIManager.class_metadata(v) for k,v in data.items() if type(v) != type([])])),
                **{'list': EDIManager.flatten(v) for k,v in data.items() if type(v) == type([])}
            }
        else:
            return EDIManager.class_metadata(data)



"""
from databricksx12.edi import *
x =  EDIManager(EDI(open("sampledata/837/CHPW_Claimdata.txt", "rb").read().decode("utf-8")))

import json
print(json.dumps(x.flatten(x.data), indent=4))

"""
