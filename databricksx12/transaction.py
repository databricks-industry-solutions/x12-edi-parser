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
    # @param is an array of segments starting with GS, ending with SE
    #
    def __init__(self, segments, delim_cls = AnsiX12Delim, fields = None, funcs = None):
        self.data = segments
        self.format_cls = delim_cls
        #self.funcs = [x for x in dir(self) if x.startswith("fx_") and x not in funcs]
        #self.fields = {**fields, **{x[3:]:getattr(self,x)() for x in self.funcs}}

    #
    # Returns number of claims in the transaction
    #
    def claim_count(self):
        return len(self.segments_by_name("CLM"))

    #
    # Returns claim level detail 
    #
    def to_claims(self):
        from databricksx12.hls.claim import Claim
        [(i, x.data) for i,x in x.segments_by_name_index("CLM")]
        """
        [(24, 'CLM*ABC11111*1800***22:B:1*Y*A*Y*Y'),
        (60, 'CLM*ABC111112*984***22:B:1*Y*A*Y*Y'),
        (94, 'CLM*ABC111113*1353***22:B:1*Y*A*Y*Y'),
        (118, 'CLM*ABC111114*1968***22:B:1*Y*A*Y*Y')]
        self.segments_by_name_index("CLM")
        """

