from databricksx12.edi import *
from databricksx12.hls.claim import *
import itertools


class HealthcareManager(EDI):

    mapping = {
            "221": Remittance, # Remittance "835"
            "222": Claim837p,
            "223": Claim837i,
            "224": None #Dental 
    }

    #
    # Given an EDI message, return a list of healthcare claims
    #
    @classmethod
    def from_edi(cls, edi):
        return cls.flatmap(cls.flatmap([cls.from_functional_group(y) for y in edi.functional_segments()]))

    @classmethod
    def flatmap(cls,x):
        return list(itertools.chain.from_iterable(x))

    @classmethod
    def from_functional_group(cls, fg):
        return [cls.from_transaction(x) for x in fg.transaction_segments()]

    #
    # Given a transaction and transaction type, return a list of healthcare data
    #  @mapping = mapping the GS08 segment to the type of healthcare transaction
    #
    @classmethod
    def from_transaction(cls, trnx):
        return ClaimBuilder(cls.mapping.get(trnx.transaction_type),
                            [x for x in trnx.data if x.segment_name() not in ['ST', 'SE']], trnx.format_cls).build()

    #
    # Convert all data to json data
    #
    @classmethod
    def to_json(cls, edi):
        return {
            **EDIManager.class_metadata(edi),
            'FunctionalGroup': [
                {
                    **EDIManager.class_metadata(fg),
                    'Transactions': [
                        {
                            **EDIManager.class_metadata(trnx),
                            'Claims': [clm.to_json() for clm in cls.from_transaction(trnx)]
                        } for trnx in fg.transaction_segments()]
                } for fg in edi.functional_segments()] 
        }

    #
    # {k,d.get(k)} for support passing in the filename for transparency / traceability 
    #
    @classmethod
    def flatten_to_json(cls, d):
        return {
            **{k:d.get(k) for k in list(d.keys()) if k not in ['EDI', 'FunctionalGroup', 'Transaction', 'Claim', 'trnx']},
            **d['EDI'],
            **d['FunctionalGroup'],
            **d['Transaction'],
            **cls.build(d['Claim'][1],
                       d['Claim'][0],
                       d['trnx'].transaction_type,
                       d['trnx'].data,
                        d['trnx'].format_cls).to_json()
        }

    #
    # Adding **kwargs to support passing in the EDI filename for transparency / traceability
    #
    @classmethod
    def flatten(cls, edi, *args, **kwargs):
        return [
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
            for clm in cls.get_claims_locations(trnx.transaction_type, trnx.data, trnx)]

    @classmethod
    def get_claims_locations(cls, transaction_type, data, trnx):
        if transaction_type in ['222', '223']:
            return trnx.segments_by_name_index(segment_name='CLM', data=data)
        elif transaction_type == '221':
            return trnx.segments_by_name_index(segment_name='CLP', data=data)
        return []
            
    @classmethod
    def build(cls, seg, i, transaction_type, data, format_cls):
        if transaction_type in ['223', '222']:
            return cls.build_claim(seg, i, cls.mapping.get(transaction_type), data, format_cls)
        elif transaction_type == '221':
            return cls.build_remittance(seg, i, cls.mapping.get(transaction_type), data, format_cls)
        return type("", (), dict({'to_json': lambda: {}}))

    @classmethod
    def build_claim(cls, seg, i, trnx_cls, data, format_cls):
        return ClaimBuilder(trnx_cls, [x for x in data if x.segment_name() not in ['SE', 'ST']], format_cls).build_claim(seg, i-1)

    @classmethod
    def build_remittance(cls, seg, i, trnx_cls, data, format_cls):
        return ClaimBuilder(trnx_cls, [x for x in data if x.segment_name() not in ['SE', 'ST']], format_cls).build_remittance(seg, i-1)
        
