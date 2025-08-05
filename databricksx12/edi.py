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
    def __init__(self, data, delim_cls = None, strict_transactions=True):
        self.raw_data = data
        self.format_cls = (self.extract_delim(data) if delim_cls is None else delim_cls)
        self.data = [Segment(x, self.format_cls) for x in data.split(self.format_cls.SEGMENT_DELIM)[:-1]]

        self._segment_index = {}
        for i, segment in enumerate(self.data):
            name = segment._name
            if name not in self._segment_index:
                self._segment_index[name] = []
            self._segment_index[name].append(i)

        self.isa = (self.segments_by_name("ISA")[0] if len(self.segments_by_name("ISA")) > 0 else Segment.empty())
        self.sender_qualifier_id = self.isa.element(5) + self.isa.element(6)
        self.recipient_qualifier_id = self.isa.element(7) + self.isa.element(8)
        self.standard_version=  self.isa.element(12)
        self.date = self.isa.element(9)
        self.time = self.isa.element(10)
        self.control_number = self.isa.element(13)

        #@param to toggle between using SE01 to parse transactions (True) or to manually search for the preceding ST segment (False)
        self._strict_transactions = strict_transactions
        self._valid_se01()


    @staticmethod
    def extract_delim(data):
        return Format(ELEMENT_DELIM= data[3:4], SEGMENT_DELIM = data[105:106], SUB_DELIM = data[104:105])

    #
    # Returns total count of segments
    #
    def segment_count(self):
        return len(self.data)

    #
    # Returns all segments matching segment_name
    #
    def segments_by_name(self, segment_name, range_start=-1, range_end=None, data = None):
        if data is not None:
            return [x for i,x in enumerate(data) if x._name == segment_name and range_start <= i <= (range_end or len(data))]

        if segment_name not in self._segment_index:
            return []
        
        indices = self._segment_index[segment_name]
        
        end = range_end or len(self.data)
        filtered_indices = [i for i in indices if range_start <= i <= end]
        
        return [self.data[i] for i in filtered_indices]

    #
    # Returns a tuple of all segments matching segment_name and their index
    #
    def segments_by_name_index(self, segment_name, range_start=-1, range_end = None, data = None):
        if data is None:
            data = self.data
        return [(i,x) for i,x in enumerate(data) if x._name == segment_name and range_start <= i <= (range_end or len(data))]

    #
    # Return the first occurence index of the specified segment
    #
    def index_of_segment(self, segments, segment_name, search_start_idx=0):
        try:
            return min([(i) for i,x in enumerate(segments) if x._name == segment_name and i >=search_start_idx])
        except:
            return -1 #not found

    #
    # Return the last occurence index of the specified segment
    #
    def last_index_of_segment(self, segments, segment_name, search_start_idx = 0):
        try:
            return max([(i) for i,x in enumerate(segments) if x._name == segment_name and i >=search_start_idx])
        except:
            return -1

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
        return [FunctionalGroup(self.segments_by_position(a, b + 1), self.format_cls, strict_transactions=self._strict_transactions) for a,b in self._functional_group_locations()]
                        

    def _functional_group_locations(self):
        gs_indices = self._segment_index.get("GS", [])
        ge_indices = self._segment_index.get("GE", [])
        return list(zip(gs_indices, ge_indices))

    def _valid_se01(self):
        if self._strict_transactions and set([self.data[i]._name for i,j in self._transaction_locations()]) != {'ST'}:
            raise Exception("SE01 segment(s) do not match to the beginning ST segment. File won't parse correctly. Is the file altered?\nTo Continue anyway, rerun with EDI(... strict_transactions=False)")
        return True
    
    #
    # Find all locations of a transaction
    #
    def _transaction_locations(self):
        if self._strict_transactions:
            se_indices = self._segment_index.get("SE", [])
            return [(i - int(self.data[i].element(1)) + 1, i) for i in se_indices]
        else:
            st_indices = self._segment_index.get("ST", [])
            se_indices = self._segment_index.get("SE", [])
            return list(zip(st_indices, se_indices))
    
    def to_json(self, exclude=["_segment_index", "data", "raw_data", "isa", "format_cls", "fg","_strict_transactions", "st", "se"]):
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
        return [{"segment_name": x._name
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

    def __getstate__(self):
        """
        Return state values to be pickled.
        Called by pickle.dumps() and cloudpickle.dumps()
        """
        return {
            'format_cls': self.format_cls,
            'data': self.data,
            'isa': self.isa,
            'sender_qualifier_id': self.sender_qualifier_id,
            'recipient_qualifier_id': self.recipient_qualifier_id,
            'standard_version': self.standard_version,
            'date': self.date,
            'time': self.time,
            'control_number': self.control_number,
            '_strict_transactions': self._strict_transactions
        }

    def __setstate__(self, state):
        """
        Restore state from the unpickled state values.
        Called by pickle.loads() and cloudpickle.loads()
        """
        self.format_cls = state['format_cls']
        self.data = state['data']
        self.isa = state['isa']
        self.sender_qualifier_id = state['sender_qualifier_id']
        self.recipient_qualifier_id = state['recipient_qualifier_id']
        self.standard_version = state['standard_version']
        self.date = state['date']
        self.time = state['time']
        self.control_number = state['control_number']
        self._strict_transactions = state['_strict_transactions']

    def __eq__(self, other):
        """
        Equality evaluation for EDI objects.
        Two EDI objects are equal if all segments in their data field are equal.
        """
        if not isinstance(other, EDI):
            return False
        
        # Check if the number of segments is the same
        if len(self.data) != len(other.data):
            return False
        
        # Compare each segment
        for i, segment in enumerate(self.data):
            if segment != other.data[i]:
                return False
        
        return True

    def __ne__(self, other):
        """
        Inequality evaluation for EDI objects.
        """
        return not self.__eq__(other)

    #reprint the EDI file as is
    def __str__(self):
        return '\n'.join([s.data for s in self.data])



class Segment():

    #
    # data 
    #
    def __init__(self, data, delim_cls = AnsiX12Delim):
        self.data = data.lstrip("\r").lstrip("\n").lstrip("\r\n")
        self.format_cls = delim_cls
        self._elements = self.data.split(self.format_cls.ELEMENT_DELIM)
        self._name = self._elements[0] if self._elements else ""


    #
    # @param element - numeric value of the field (starting at 0)
    # @param sub_element - get a sub-element value (-1 for all, 0 through N for sub number)
    # @param dne - "Does Not Exist" value to return if  the element or sub element requested exceeds what is there
    # @returns a single element or sub element string
    #
    def element(self, element, sub_element=-1, dne=""):
        try:
            if sub_element == -1:
                return self._elements[element]
            else:
                return self._elements[element].split(self.format_cls.SUB_DELIM)[sub_element]
        except:
            return dne

    #
    # @returns number of elements in a segment 
    #
    def segment_len(self):
        return len(self._elements)

    #
    # @returns the number of sub elements in a segment
    #
    def sub_element_len(self, element = 0):
        return len(self._elements[element].split(self.format_cls.SUB_DELIM))

    #
    # Filter this segment for element/sub_element values
    #
    def filter(self, value, element, sub_element, dne=""):
        return self if value == self.element(element, sub_element, dne) else None

    @classmethod
    def empty(cls):
        return cls(data="")

    def __getstate__(self):
        """
        Return state values to be pickled.
        Called by pickle.dumps() and cloudpickle.dumps()
        """
        return {
            'data': self.data,
            'format_cls': self.format_cls
        }

    def __setstate__(self, state):
        """
        Restore state from the unpickled state values.
        Called by pickle.loads() and cloudpickle.loads()
        """
        self.data = state['data']
        self.format_cls = state['format_cls']
        self._elements = self.data.split(self.format_cls.ELEMENT_DELIM)
        self._name = self._elements[0] if self._elements else ""

    def __eq__(self, other):
        """
        Equality evaluation for Segment objects.
        Two Segment objects are equal if their data field (string) is equal.
        """
        if not isinstance(other, Segment):
            return False
        
        return self.data == other.data

    def __ne__(self, other):
        """
        Inequality evaluation for Segment objects.
        """
        return not self.__eq__(other)


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
    def class_metadata(cls_obj):
        return cls_obj.to_json()

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

