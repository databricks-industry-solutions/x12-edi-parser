
#
# 
#
class Format():
    def __init__(self):
        pass

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
