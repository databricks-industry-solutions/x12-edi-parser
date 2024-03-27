from databricksx12.edi import *
from databricksx12.hls.claim import *
import itertools


class HealthcareManager(EDI):

    def __init__(self, mapping = {
            "222": "837P",
            "223": "837I",
            "221": "835"
    }):
        self.mapping = mapping


    #
    # Given an EDI message, return a list of healthcare claims
    #
    def from_edi(self, edi):
        return list(itertools.chain.from_iterable([self.from_functional_group(y) for y in edi.functional_segments()])) 


    def from_functional_group(self, fg):
        return [self.from_transaction(x) for x in fg.transaction_segments()]

    #
    # Given a transaction and transaction type, return a list of healthcare data
    #  @mapping = mapping the GS08 segment to the type of healthcare transaction
    #
    def from_transaction(self, trnx):
        type = self.mapping.get(trnx.transaction_type)
        data = [x for x in trnx.data if x.segment_name() not in ['ST', 'SE']]
        if type == "837P": 
            return Claim837p(data, trnx.format_cls)
        elif type == "837I":
            return Claim837i(data, trnx.format_cls)
        else:
            return None #no mapping available
    
