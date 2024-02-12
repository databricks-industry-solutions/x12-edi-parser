    
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
