
#https://www.iri.com/blog/test-data/synthesizing-realistic-hl7-and-x12-test-data/#:~:text=In%20HL7%2C%20a%20message%20segment,with%20preceding%20and%20trailing%20segments.

class HL7(EDI):

    def __init__(self, segments, delim_cls = AnsiX12Delim, fields = None, funcs = None):
        self.data = segments
        self.format_cls = delim_cls
        
