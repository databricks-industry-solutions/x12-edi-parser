import re, functools
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
        self.funcs = [x for x in dir(self) if x.startswith("fx_")] 
        self.fields = {x[3:]:getattr(self,x)() for x in self.funcs}
        
    #
    # Returns total count of segments
    #
    def segment_count(self):
        return len(self.data)

    #
    # Returns all segments matching segment_name
    #
    def segments_by_name(self, segment_name, range_start=-1, range_end=None):
        return [x for i,x in enumerate(self.data) if x.segment_name() == segment_name and range_start <= i <= (range_end or len(self.data))]

    #
    # Returns a tuple of all segments matching segment_name and their index
    #
    def segments_by_name_index(self, segment_name, range_start=-1, range_end = None):
        return [(i,x) for i,x in enumerate(self.data) if x.segment_name() == segment_name and range_start <= i <= (range_end or len(self.data))]
    
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
    # Return all segments associated with each transaction
    #  [ trx1[SEGMENT1, ... SEGMENTN], trx2[SEGMENT1, ... SEGMENTN] ... ]
    #  SE01 element contains how many segments are included in the transaction
    #
    def transaction_segments(self):
        from databricksx12.transaction import Transaction
        return [Transaction(self.segments_by_position(a,b), self.format_cls, self.fields, self.funcs) for a,b in self._transaction_locations()]

    def _transaction_locations(self):
        return [(i - int(x.element(1))+1,i+1) for i,x in  self.segments_by_name_index("SE")]
    
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
    
    """
     spark dataframe can be built from json
    """
    def toJson(self):
        return self.fields
    
    #
    # e.g. 835 -> 221 according to https://www.cgsmedicare.com/pdf/edi/835_compguide.pdf
    # 
    def fx_edi_transaction_type(self):
        if self.segments_by_name("GS")[0].element(8)[7:10] == '222':
            return '837P'
        elif self.segments_by_name("GS")[0].element(8)[7:10] == '223':
            return '837I'
        elif self.segments_by_name("GS")[0].element(8)[7:10] == '221':
            return '835'
        else:
            return 'not implemented error for segment GS08 ' + self.segments_by_name("GS")[0].element(8)
        
    def fx_transaction_datetime(self):
        return self.segments_by_name("GS")[0].element(4) + ":" + self.segments_by_name("GS")[0].element(5)
    
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
    def element(self, element, sub_element=-1, dne="na/dne"):
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
    def filter(self, value, element, sub_element, dne="na/dne"):
        return self if value == self.get_element(element, sub_element, dne) else None

    
