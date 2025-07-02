
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

    def __getstate__(self):
        """
        Return state values to be pickled.
        Called by pickle.dumps() and cloudpickle.dumps()
        """
        return {
            'SEGMENT_DELIM': self.SEGMENT_DELIM,
            'ELEMENT_DELIM': self.ELEMENT_DELIM,
            'SUB_DELIM': self.SUB_DELIM,
            'class_name': self.__class__.__name__
        }

    def __setstate__(self, state):
        """
        Restore state from the unpickled state values.
        Called by pickle.loads() and cloudpickle.loads()
        """
        self.SEGMENT_DELIM = state['SEGMENT_DELIM']
        self.ELEMENT_DELIM = state['ELEMENT_DELIM']
        self.SUB_DELIM = state['SUB_DELIM']

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
