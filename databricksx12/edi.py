import re
from databricksx12.format import *

#
# 
#
class EDI():

    #
    # @param df - the Dataframe of EDI transactions
    # @param column_name - the column containing the UTF-8 edi data
    # @param delim_class - class that contains the delimiter information for parsing the EDI transactions
    #             - AnsiX12Delim is the default and most used
    def __init__(self, df, column_name = "raw_data", delim_cls = AnsiX12Delim):
        self.df = df
        self.format_cls = delim_cls
        self.column = column_name

    #
    # Return edi file delimiter (leading _ => class internal function)
    #
    def _get_delimiter(self):
        pass


    #
    # @param segment_name - the segment to search for within an EDI
    # @returns - an dataframe containing an array of 0*N of segments
    #
    def get_segments(self, segment_name):
        return (self.df.select(self.column).rdd
                .map(lambda x: x[self.column].split(self.format_cls.SEGMENT_DELIM))
                .map(lambda x: [y for y in x if y.startswith(segment_name)])
                .map(lambda x: self.format_cls.SEGMENT_DELIM.join(x))
                ).toDF([segment_name])

    #
    # @returns - header class object from EDI
    #
    def get_header(self):
        pass


class Segment():

    #
    # data 
    #
    def __init__(self, data, delim_cls = AnsiX12Delim):
        self.data = data
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

