import re
from databricksx12.format import *
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql import Row

#
# 
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

    #
    # Returns total count of segments
    #
    def segment_count(self):
        return len(self.data)

    #
    # Returns all segments matching segment_name
    #
    def get_segments_by_name(self, segment_name):
        return [x for x in self.data if x.segment_name() == segment_name]

    #
    # @param position_start - integer, the first segment to include (inclusive) starting at 0
    # @param position_end - integer, the last segment to include (exclusive) starting at 0
    #
    # @returns - all segements between start and end positions
    #          - if end_position is beyond last segment, returns up until last segment 
    #
    def get_segments_by_position(self, position_start, position_end):
        return self.data[position_start:position_end]

    #
    # @returns - header class object from EDI
    #
    def get_header(self):
        return self.data[0]

    #
    # 
    #
    def position_of(self, segment_name):
        pass

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
    def get_element(self, element, sub_element=-1, dne="na/dne"):
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
    def element_len(self):
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
