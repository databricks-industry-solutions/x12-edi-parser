from databricksx12.edi import *

class FunctionalGroup(EDI):

    def __init__(self, segments, delim_cls = AnsiX12Delim, fields = None, funcs = None):
        self.data = segments
        self.format_cls = delim_cls
        self.transaction_type = self._transaction_type()
        self.transaction_datetime = self._transaction_datetime()

    #
    # e.g. 835 -> 221 according to https://www.cgsmedicare.com/pdf/edi/835_compguide.pdf
    # 
    def _transaction_type(self):
        if self.segments_by_name("GS")[0].element(8)[7:10] == '222':
            return '837P'
        elif self.segments_by_name("GS")[0].element(8)[7:10] == '223':
            return '837I'
        elif self.segments_by_name("GS")[0].element(8)[7:10] == '221':
            return '835'
        else:
            return 'not implemented error for segment GS08 ' + self.segments_by_name("GS")[0].element(8)


    def _transaction_datetime(self):
        return self.segments_by_name("GS")[0].element(4) + ":" + self.segments_by_name("GS")[0].element(5)
