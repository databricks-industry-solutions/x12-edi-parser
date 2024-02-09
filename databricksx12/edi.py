    
#
# 
#
class EDI():

    #
    # @param df - the Dataframe of EDI transactions
    # @param column_name - the column containing the UTF-8 edi data
    # @param delim_class - class that contains the delimiter information for parsing the EDI transactions
    #             - AnsiX12Delim is the default and most used
    def __init__(self, df, column_name = "raw_data", delim_class = AnsiX12Delim):
        self.df = df
        self.format_cls = delim_class
        self.column = column_name
        #self.??? = ???
        #self.??? = ??? 


    #
    # Return edi file delimiter (leading _ => class internal function)
    #
    def _get_delimiter(self):
        pass


    #
    # @param segment_name - the segment to search for within an EDI
    # @returns - an array of 0 or more segments that match the segment name
    #
    def get_segments(self, segment_name):
        pass 

    #
    # @returns - header class object from EDI
    #
    def get_header(self):
        pass


class MsgHeader():

    #
    #
    #
    def __init__(self, data):
        self.data = data

    #
    #
    #
    def get_msg_separator(self):
        pass 

class Segment():

    def __init__(self, data):
        self.data = data

    #
    # @param field_number - numeric value of the field
    # @param delim - the delimiter used to separate values from the segment  
    # @return the value in the field from the specified segment 
    #
    def get_field(self, field_number, delim):
        pass

    #
    # @param delimiter to use
    # @returns number of elements in a segment 
    #
    def get_length(self, delim):
        pass 
