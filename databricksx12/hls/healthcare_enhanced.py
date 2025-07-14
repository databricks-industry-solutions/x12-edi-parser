from databricksx12.edi import *
from databricksx12.hls.claim_enhanced import *
import itertools

class HealthcareManager(EDI):
    def __init__(self, mapping = {
            "223": Claim837i, # 837I Institutional
            "222": Claim837p, # 837P Professional
    }):
        self.mapping = mapping

    def from_edi(self, edi):
        return self.flatmap(self.flatmap([self.from_functional_group(y) for y in edi.functional_segments()]))

    def flatmap(self,x):
        return list(itertools.chain.from_iterable(x))

    def from_functional_group(self, fg):
        return [self.from_transaction(x) for x in fg.transaction_segments()]

    def from_transaction(self, trnx):
        return ClaimBuilder(self.mapping.get(trnx.transaction_type),
                            [x for x in trnx.data if x.segment_name() not in ['ST', 'SE']], trnx.format_cls).build()

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

    def flatten_to_denormalized_json(self, d):
        """Enhanced flattening for complete demographic capture"""
        claim_data = self.build(
            d['Claim'][1], 
            d['Claim'][0], 
            d['trnx'].transaction_type, 
            d['trnx'].data, 
            d['trnx'].format_cls
        )
        
        # Get file metadata
        filename = d.get('filename', '')
        transaction_control_number = d.get('Transaction', {}).get('transaction_control_number', '')
        
        # Return completely denormalized structure
        return claim_data.to_denormalized_json(filename, transaction_control_number)

    def flatten(self, edi, *args, **kwargs):
        res =  [
            {
                **kwargs,
                'EDI': EDIManager.class_metadata(edi),
                'FunctionalGroup': EDIManager.class_metadata(fg),
                'Transaction': EDIManager.class_metadata(trnx),
                'Claim': clm,
                'trnx': trnx
            }
            for fg in edi.functional_segments()
            for trnx in fg.transaction_segments()
            for clm in self.get_claims_locations(trnx.transaction_type, trnx.data, trnx)]
        return res

    def get_claims_locations(self, transaction_type, data, trnx):
        if transaction_type in ['222', '223']:
            return trnx.segments_by_name_index(segment_name='CLM', data=data)
        return []
            
    def build(self, seg, i, transaction_type, data, format_cls):
        if transaction_type in ['223', '222']:
            return self.build_claim(seg, i, self.mapping.get(transaction_type), data, format_cls)
        return type("", (), dict({'to_denormalized_json': lambda filename='', tcn='': {}}))

    def build_claim(self, seg, i, trnx_cls, data, format_cls):
        seg_data = [x for x in data if x.segment_name() not in ['SE', 'ST']]
        return ClaimBuilder(trnx_cls, seg_data, format_cls).build_claim(seg, i-1)