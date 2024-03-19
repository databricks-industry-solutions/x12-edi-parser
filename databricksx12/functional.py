from databricksx12.edi import *

class FunctionalGroup(EDI):

    def __init__(self, segments, delim_cls = AnsiX12Delim, fields = None, funcs = None):
        self.data = segments
        self.format_cls = delim_cls


    
