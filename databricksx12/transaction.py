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
    def __init__(self, segments, delim_cls = AnsiX12Delim):
        self.data = segments
        self.format_cls = delim_cls

#
# Interface for creating transactions from functional groups
#
class TransactionManager():

    def __init__(self, fgs):
        self.fgs = fgs

    
