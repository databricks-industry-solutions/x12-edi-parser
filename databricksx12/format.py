class Format():
    def __init__(self):
        pass

#
#  https://www.ibm.com/docs/en/sgfmw/5.3.1?topic=gs-edi-delimiters
#  https://github.com/databrickslabs/dbignite/blob/main/dbignite/readers.py#L1-L9
#
class AnsiX12Delim(Format):
    AnsiX12Delim.SEGMENT_DELIM = "~"
    AnsiX12Delim.ELEMENT_DELIM = "*"
    AnsiX12Delim.SUB_DELIM = ":"

    def __init__(self):
        pass

class EDIFactDelim(Format):
    EDIFactDelim.TAG_DELIM = "+"
    EDIFactDelim.SEGMENT_DELIM = "'"
    EDIFactDelim.ELEMENT_DELIM = "+"
    EDIFactDelim.SUB_DELIM = ":"
    EDIFactDelim.DECIMAL_DELIM = "."
    EDIFactDelim.RELEASE_DELIM = "?"    

    def __init__(self):
        pass

class TradacomsDelim(self):
    TradacomsDelim.TAG_DELIM = "="
    TradacomsDelim.SEGMENT_DELIM = "'"
    TradacomsDelim.ELEMENT_DELIM = "+"
    TradacomsDelim.SUB_DELIM = ":"
    TradacomsDelim.DECIMAL_DELIM = "."
    TradacomsDelim.RELEASE_DELIM = "?"
    def __init__(self):
        pass


