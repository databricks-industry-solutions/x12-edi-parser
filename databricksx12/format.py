
#
# 
#
class Format(dict):
    __getattr__, __setattr__ = dict.get, dict.__setitem__

    def __init__(self, SEGMENT_DELIM, ELEMENT_DELIM, SUB_DELIM):
        self.SEGMENT_DELIM = SEGMENT_DELIM
        self.ELEMENT_DELIM = ELEMENT_DELIM
        self.SUB_DELIM = SUB_DELIM

    def __eq__(self, other):
        try:
            return self.SEGMENT_DELIM == other.SEGMENT_DELIM and self.ELEMENT_DELIM == other.ELEMENT_DELIM and self.SUB_DELIM == other.SUB_DELIM
        except:
            return False

#
#  https://www.ibm.com/docs/en/sgfmw/5.3.1?topic=gs-edi-delimiters
#  https://github.com/databrickslabs/dbignite/blob/main/dbignite/readers.py#L1-L9
#
class AnsiX12Delim(Format):
    SEGMENT_DELIM = "~"
    ELEMENT_DELIM = "*"
    SUB_DELIM = ":"

class EDIFactDelim(Format):
    TAG_DELIM = "+"
    SEGMENT_DELIM = "'"
    ELEMENT_DELIM = "+"
    SUB_DELIM = ":"
    DECIMAL_DELIM = "."
    RELEASE_DELIM = "?"    

class TradacomsDelim(Format):
    TAG_DELIM = "="
    SEGMENT_DELIM = "'"
    ELEMENT_DELIM = "+"
    SUB_DELIM = ":"
    DECIMAL_DELIM = "."
    RELEASE_DELIM = "?"
