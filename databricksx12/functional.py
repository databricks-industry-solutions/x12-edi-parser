from databricksx12.edi import *

class FunctionalGroup(EDI):

    def __init__(self, segments, delim_cls = AnsiX12Delim, strict_transactions = True):
        self.data = segments
        self.format_cls = delim_cls
        self.transaction_type = self._transaction_type()
        self.fg = (self.segments_by_name("GS")[0] if len(self.segments_by_name("GS")) > 0 else Segment.empty())
        self.standard_version = self.fg.element(8)
        self.control_number = self.fg.element(6)
        self.date = self.fg.element(4)
        self.time =  self.fg.element(5)
        self.sender = self._sender()
        self.receiver = self._reciever()
        self._strict_transactions = strict_transactions
        
    #
    # Return all segments associated with each transaction
    #  [ trx1[SEGMENT1, ... SEGMENTN], trx2[SEGMENT1, ... SEGMENTN] ... ]
    #  SE01 element contains how many segments are included in the transaction
    #
    def transaction_segments(self):
        from databricksx12.transaction import Transaction
        return [Transaction(self.segments_by_position(a,b), self.format_cls, self.transaction_type) for a,b in self._transaction_locations()]


    #
    # e.g. 835 -> 221 according to https://www.cgsmedicare.com/pdf/edi/835_compguide.pdf
    # 
    def _transaction_type(self):
        return self.segments_by_name("GS")[0].element(8)[7:10]

    def _sender(self):
        return self.segments_by_name("GS")[0].element(2)

    def _reciever(self):
        return self.segments_by_name("GS")[0].element(3)

    def __getstate__(self):
        """
        Return state values to be pickled.
        Called by pickle.dumps() and cloudpickle.dumps()
        """
        return {
            'data': self.data,
            'format_cls': self.format_cls,
            'transaction_type': self.transaction_type,
            'fg': self.fg,
            'standard_version': self.standard_version,
            'control_number': self.control_number,
            'date': self.date,
            'time': self.time,
            'sender': self.sender,
            'receiver': self.receiver,
            '_strict_transactions': self._strict_transactions
        }

    def __setstate__(self, state):
        """
        Restore state from the unpickled state values.
        Called by pickle.loads() and cloudpickle.loads()
        """
        self.data = state['data']
        self.format_cls = state['format_cls']
        self.transaction_type = state['transaction_type']
        self.fg = state['fg']
        self.standard_version = state['standard_version']
        self.control_number = state['control_number']
        self.date = state['date']
        self.time = state['time']
        self.sender = state['sender']
        self.receiver = state['receiver']
        self._strict_transactions = state['_strict_transactions']