from databricksx12.edi import Segment
from typing import List, Dict
import itertools
from collections import defaultdict
from functools import reduce

class Identity:

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}
    

#
# Hanlde providers across 837i and 837p claims
#  - Billing, servicing, facility, attending, operating, other
#
class ProviderIdentity(Identity):

    def __init__(self, nm1=Segment.empty(), n3=Segment.empty(), n4=Segment.empty(), ref = Segment.empty(), prv = Segment.empty()):
        self.npi = nm1.element(9)
        self.entity_type = 'Organization' if nm1.element(2) == '2' else 'Individual'
        self.name = nm1.element(3) if self.entity_type == 'Organization' else ' '.join([nm1.element(3), nm1.element(4,dne=""), nm1.element(5)])
        self.street = n3.element(1)
        self.city = n4.element(1)
        self.state = n4.element(2)
        self.zip = n4.element(3)
        self.ein_type = ref.element(1)
        self.ein = ref.element(2)
        self.taxonomy = prv.element(3)
        self.provider_role = prv.element(1)
        
class PayerIdentity(Identity):
    def __init__(self, nm1):
        self.payer_name = nm1.element(3)
        self.payer_identifier = nm1.element(9)
        self.payer_identifier_cd = nm1.element(8)
        self.business_entity_type = nm1.element(2)

class PatientIdentity(Identity):
        def __init__(self, nm1, n3, n4, dmg, pat, sbr, ref = Segment.empty()):
            self.subsciber_identifier = nm1.element(9)
            self.name = ' '.join([nm1.element(3), nm1.element(4), nm1.element(5)])
            self.patient_relationship_cd = pat.element(1)
            self.subscriber_relationship_cd = sbr.element(2)
            self.street = n3.element(1)
            self.city = n4.element(1)
            self.state = n4.element(2)
            self.zip = n4.element(3)
            self.dob = dmg.element(2)
            self.dob_format = dmg.element(1)
            self.gender_cd = dmg.element(3)
            self.mrn = ref.element(2)

class ClaimIdentity(Identity):
    #
    # clm, cl1 are individual segments
    # dtp is a loop of 0 or more dates 
    #
    def __init__(self, clm, dtp, cl1 = Segment.empty(), k3 = Segment.empty(), hi = Segment.empty(), ref = [], amt = [],  principal_hi = Segment.empty(), other_hi = []):
        self.claim_id = clm.element(1)
        self.claim_amount = clm.element(2)
        self.facility_type_code = clm.element(5)
        self.claim_dates = [{'date_cd': s.element(1), 'date_format': s.element(2), 'date': s.element(3)} for s in dtp]
        self.admission_type = cl1.element(1)
        self.admission_src_cd = cl1.element(2)
        self.discharge_status_cd = cl1.element(3)
        self.encounter_id = k3.element(1)
        self.drg_cd = hi.element(1)
        self.clm_refs = [{'id_code_qualifier': x.element(1), 'id': x.element(2)} for x in ref]
        self.other_amts = [{'amt_qualifier_cd': a.element(1), 'amt': a.element(2)} for a in amt]
        self.icd10_pcs_cds = { #only 837i
            'principal_prcdr': {'prcdr_cd': principal_hi.element(1,1), 'date_format': principal_hi.element(1,2), 'date': principal_hi.element(1,3)},
            'other_cds': list(itertools.chain(*[
                [{'prcdr_cd': s.element(i,1), 'date_format': s.element(i,2), 'date': s.element(i,3)} for i in list(range(1, s.segment_len()))]
            for s in other_hi])) 
        }

# POA is the last sub element of the respective segments
class DiagnosisIdentity(Identity):
    def __init__(self, hi_segments):
        self.principal_dx_cd = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABK"] == [] else [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABK"][0]
        self.principal_dx_poa = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABK"] == [] else [s.element(1,s.sub_element_len(1)-1) for s in hi_segments if s.element(1,0) == "ABK"][0]
        self.admitting_dx_cd = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABJ"] == []	else [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABJ"][0]
        self.admitting_dx_poa = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABJ"] == []	else [s.element(1,s.sub_element_len(1)-1) for s in hi_segments if s.element(1,0) == "ABJ"][0]
        self.reason_visit_dx_cd = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "APR"] == [] else [s.element(1,1) for s in hi_segments if s.element(1, 0) == "APR"][0]
        self.reason_visit_dx_poa = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "APR"] == [] else [s.element(1,s.sub_element_len(1)-1) for s in hi_segments if s.element(1,0) == "APR"][0]
        self.external_injury_dx_cd = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABN"] == [] else [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABN"][0]
        self.external_injury_dx_poa = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABN"] == [] else [s.element(1,s.sub_element_len(1)-1) for s in hi_segments if s.element(1,0) == "ABN"][0]
        self.other_dx_cds = list(itertools.chain(*[[{'dx_cd': s.element(i,1), 'poa': s.element(i,s.sub_element_len(i)-1)} for i in list(range(1, s.segment_len())) if s.element(i,0) == "ABF"]
            for s in hi_segments]))
    
class Submitter_Receiver_Identity(Identity):
    def __init__(self, nm1=Segment.empty(), per= Segment.empty()):
        self.type = 'Organization' if nm1.element(2) == '2' else 'Individual'
        self.name = nm1.element(3) if self.type == 'Organization' else ' '.join([nm1.element(3), nm1.element(4,dne=""), nm1.element(5)])
        if per.element(0):
            self.sbmtter_contact_name = "" if per.element(2) == '' else per.element(2)
            self.sbmtter_contacts = "" if (per.element(3), per.element(4)) == [] else [(per.element(3), per.element(4)), #telephone
                                                                                    (per.element(5), per.element(6))] # usually fax/email

                
class ServiceLine(Identity):

    def __init__(self, d):
        for k,v in d.items():
            setattr(self,k,v)

    @staticmethod
    def common(sv, lx, dtp, amt):
        return {
            "claim_line_number": lx.element(1),
            "service_dates": [{'date_cd': s.element(1), 'date_format': s.element(2), 'date': s.element(3)} for s in dtp],
            'other_amts': [{'amt_qualifier_cd': a.element(1), 'amt': a.element(2)} for a in amt]
        }

    #
    # Institutional Claims
    #
    @classmethod
    def from_sv2(cls, sv2, lx, dtp, amt):
        return cls({**cls.common(sv2, lx, dtp,amt),
                    **{
                        "units": sv2.element(5),
                        "units_measurement": sv2.element(4),
                        "line_chrg_amt": sv2.element(3),
                        "prcdr_cd": sv2.element(2, 1, ""),
                        "prcdr_cd_type": sv2.element(2, 0, ""),
                        "modifier_cds": ','.join(filter(lambda x: x!="", [sv2.element(2, 2, ""), sv2.element(2, 3, ""), sv2.element(2, 4,""), sv2.element(2, 5, "")])),
                        "revenue_cd": sv2.element(1),
                        "service_dates": [{'date_cd': s.element(1), 'date_format': s.element(2), 'date': s.element(3)} for s in dtp]
                    }
                })

    #
    # Professional Claims
    #
    @classmethod
    def from_sv1(cls, sv1, lx, dtp, amt):
        return cls({**cls.common(sv1, lx, dtp, amt),
                    **{
                        "units": sv1.element(4),
                        "units_measurement": sv1.element(3),
                        "line_chrg_amt": sv1.element(2),
                        "prcdr_cd": sv1.element(1, 1),
                        "prcdr_cd_type": sv1.element(1, 0),
                        "modifier_cds": ','.join(filter(lambda x: x!="", [sv1.element(1, 2, ""), sv1.element(1, 3, ""), sv1.element(1, 4,""), sv1.element(1, 5, "")])),
                        "place_of_service": sv1.element(5),
                        "dg_cd_pntr": sv1.element(7),
                        "service_dates": [{'date_cd': s.element(1), 'date_format': s.element(2), 'date': s.element(3)} for s in dtp]
                    }
                })
        

        
