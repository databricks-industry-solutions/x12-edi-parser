from databricksx12.edi import *

#
# Base class for all types of transactions. Each transaction must be a subclass of this
#
class Transaction():

    #
    # @param is an array of segments starting with GS, ending with SE
    #
    def __init__(self, segments, delim_cls = AnsiX12Delim, funcs = None):
        self.segments = segments
        self.format_cls = delim_cls
        
        #self.funcs = [x for x in dir(self) if x.startswith("fx_")] if funcs is None else funcs 
        #self.fields = {x[-3:]:getattr(self,x) for x in self.funcs}

    

    #
    # Override this method for each subclass 
    #  Could add transaction types, etc here if desired 
    def toJson(self):
        return {'transaction': self.format_cls.SEGMENT_DELIM.join(self.segments)}

    

"""
 Parsing 837 for relevant information about a transaction.
  - @param "funcs" define which functions to use to flatten a transaction. Default is to use all "fx_*" definitions 
  - @self.fields is a key/value pair dict which stores the function name (minus fx_ prefix) and return value
     - @self.fields can easily be converted to a pyspark DataFrame for analysis
"""
class Claim(Transaction):

    def __init__(self):
        pass


    #
    # Determine the transaction type, 837I, 837P, 835 etc...
    #
    def fx_edi_transaction_type(self):
        pass
