from databricksx12.edi import *
from databricksx12.hls.claim import *
import itertools


class HealthcareManager(EDI):

    def __init__(self, mapping = {
            "221": Remittance, # Remittance "835"
            "222": Claim837p,
            "223": Claim837i,
            "224": None #Dental 

    }):
        self.mapping = mapping


    #
    # Given an EDI message, return a list of healthcare claims
    #
    def from_edi(self, edi):
        return self.flatmap(self.flatmap([self.from_functional_group(y) for y in edi.functional_segments()]))

    def flatmap(self,x):
        return list(itertools.chain.from_iterable(x))

    def from_functional_group(self, fg):
        return [self.from_transaction(x) for x in fg.transaction_segments()]

    #
    # Given a transaction and transaction type, return a list of healthcare data
    #  @mapping = mapping the GS08 segment to the type of healthcare transaction
    #
    def from_transaction(self, trnx):
        return ClaimBuilder(self.mapping.get(trnx.transaction_type),
                            [x for x in trnx.data if x.segment_name() not in ['ST', 'SE']], trnx.format_cls).build()

    #
    # Convert all data to json data
    #
    def to_json(self, edi):
        return {
            **EDIManager.class_metadata(edi),
            'FunctionalGroup': [
                {
                    **EDIManager.class_metadata(fg),
                    'Transactions': [
                        {
                            **EDIManager.class_metadata(trnx),
                            'Claims': [clm.to_json() for clm in self.from_transaction(trnx)]
                        } for trnx in fg.transaction_segments()]
                } for fg in edi.functional_segments()] 
        }

    def flatten_to_json(self, d):
        return {
            **d['EDI'],
            **d['FunctionalGroup'],
            **d['Transaction'],
            **self.build(d['Claim'][1],
                       d['Claim'][0],
                       d['trnx'].transaction_type,
                       d['trnx'].data,
                       d['trnx'].format_cls).to_json()
        }
    
    def flatten(self, edi):
        return [
            {
                'EDI': EDIManager.class_metadata(edi),
                'FunctionalGroup': EDIManager.class_metadata(fg),
                'Transaction': EDIManager.class_metadata(trnx),
                'Claim': clm,
                'trnx': trnx
            }
            for fg in edi.functional_segments()
            for trnx in fg.transaction_segments()
            for clm in self.get_claims_locations(trnx.transaction_type, trnx.data, trnx)]

    def get_claims_locations(self, transaction_type, data, trnx):
        if transaction_type in ['222', '223']:
            return trnx.segments_by_name_index(segment_name='CLM', data=data)
        elif transaction_type == '221':
            return trnx.segments_by_name_index(segment_name='CLP', data=data)
        return []
            
    def build(self, seg, i, transaction_type, data, format_cls):
        if transaction_type in ['223', '222']:
            return self.build_claim(seg, i, self.mapping.get(transaction_type), data, format_cls)
        elif transaction_type == '221':
            return self.build_remittance(seg, i, self.mapping.get(transaction_type), data, format_cls)
        return {}


    def build_claim(self, seg, i, trnx_cls, data, format_cls):
        return ClaimBuilder(trnx_cls, [x for x in data if x.segment_name() not in ['SE', 'ST']], format_cls).build_claim(seg, i)

    def build_remittance(self, seg, i, trnx_cls, data, format_cls):
        return ClaimBuilder(trnx_cls, [x for x in data if x.segment_name() not in ['SE', 'ST']], format_cls).build_remittance(seg, i)
        
