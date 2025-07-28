from databricksx12.edi import *
from databricksx12.format import *

"""
 Base class for all transactions (ST/SE Segments)

  Building a Spark DataFrame using toJson()
  - @param "funcs" define which functions to use to flatten a transaction. Default is to use all "fx_*" definitions 
  - @self.fields is a key/value pair dict which stores the function name (minus fx_ prefix) and return value
     - @self.fields converted to a pyspark DataFrame for analytics
    
"""
class Transaction(EDI):

    #
    # @param segments is expected as an array of segments starting with ST, ending with SE
    #
    def __init__(self,segments, delim_cls = AnsiX12Delim, transaction_type=None):
        self.data = segments
        self.format_cls = delim_cls
        self._strict_transactions = True

        self._segment_index = {}
        for i, segment in enumerate(self.data):
            name = segment._name
            if name not in self._segment_index:
                self._segment_index[name] = []
            self._segment_index[name].append(i)

        self.st = self.segments_by_name("ST")[0]
        self.se = self.segments_by_name("SE")[0]
        self.transaction_set_code = self.st.element(1)
        self.control_number = self.st.element(2)
        self.transaction_type = transaction_type

    def __getstate__(self):
        """
        Return state values to be pickled.
        Called by pickle.dumps() and cloudpickle.dumps()
        """
        return {
            'format_cls': self.format_cls,
            'data': self.data,
            'transaction_type': self.transaction_type,
            'st': self.st,
            'se': self.se,
            'transaction_set_code': self.transaction_set_code,
            'control_number': self.control_number,
            '_strict_transactions': self._strict_transactions
        }

    def __setstate__(self, state):
        """
        Restore state from the unpickled state values.
        Called by pickle.loads() and cloudpickle.loads()
        """
        self.format_cls = state['format_cls']
        self.data = state['data']
        self.transaction_type = state['transaction_type']
        self.st = state['st']
        self.se = state['se']
        self.transaction_set_code = state['transaction_set_code']
        self.control_number = state['control_number']
        self._strict_transactions = state['_strict_transactions']
