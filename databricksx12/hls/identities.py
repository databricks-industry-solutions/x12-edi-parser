from databricksx12.edi import Segment
from typing import List, Dict
import itertools
from collections import defaultdict
from functools import reduce

class Identity:

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
    def _extract_segments(self, segments: List[Segment]):
        """
        Extract all segments dynamically and store them in self.segments dictionary.
        Format: self.segments['SEGMENT_NAME'] = [list of element lists]
        Example: self.segments['REF'] = [['1W', '123'], ['SY', '999']]
        """
        self.segments = {}
        for seg in segments:
            if seg and hasattr(seg, '_name') and seg._name:  # Skip empty segments
                seg_name = seg._name
                # Extract all elements from the segment
                elements = []
                seg_len = seg.segment_len()
                for i in range(seg_len):
                    element_value = seg.element(i)
                    # Check if element has sub-elements
                    if element_value and hasattr(seg, 'format_cls') and seg.format_cls and seg.format_cls.SUB_DELIM in element_value:
                        sub_elements = element_value.split(seg.format_cls.SUB_DELIM)
                        elements.append(sub_elements)
                    else:
                        elements.append(element_value)
                
                # Store in segments dictionary
                if seg_name not in self.segments:
                    self.segments[seg_name] = []
                self.segments[seg_name].append(elements)
    

#
# Hanlde providers across 837i and 837p claims
#  - Billing, servicing, facility, attending, operating, other
#
class ProviderIdentity(Identity):

    def __init__(self, segments=None, nm1=Segment.empty(), n3=Segment.empty(), n4=Segment.empty(), ref = Segment.empty(), prv = Segment.empty()):
        # If segments list is provided, use dynamic extraction
        if segments is not None:
            self._extract_segments(segments)
            # Extract common fields for backward compatibility
            nm1_seg = next((s for s in segments if s._name == "NM1"), Segment.empty())
            n3_seg = next((s for s in segments if s._name == "N3"), Segment.empty())
            n4_seg = next((s for s in segments if s._name == "N4"), Segment.empty())
            ref_seg = next((s for s in segments if s._name == "REF"), Segment.empty())
            prv_seg = next((s for s in segments if s._name == "PRV"), Segment.empty())
        else:
            # Backward compatibility: use individual segments
            self._extract_segments([s for s in [nm1, n3, n4, ref, prv] if s and s._name])
            nm1_seg = nm1
            n3_seg = n3
            n4_seg = n4
            ref_seg = ref
            prv_seg = prv
        
        # Extract common fields for backward compatibility
        self.npi = nm1_seg.element(9)
        self.entity_type = 'Organization' if nm1_seg.element(2) == '2' else 'Individual'
        self.name = nm1_seg.element(3) if self.entity_type == 'Organization' else ' '.join([nm1_seg.element(3), nm1_seg.element(4,dne=""), nm1_seg.element(5)])
        self.street = n3_seg.element(1)
        self.city = n4_seg.element(1)
        self.state = n4_seg.element(2)
        self.zip = n4_seg.element(3)
        self.ein_type = ref_seg.element(1)
        self.ein = ref_seg.element(2)
        self.taxonomy = prv_seg.element(3)
        self.provider_role = prv_seg.element(1)
        
class PayerIdentity(Identity):
    def __init__(self, segments=None, nm1=Segment.empty()):
        # If segments list is provided, use dynamic extraction
        if segments is not None:
            self._extract_segments(segments)
            nm1_seg = next((s for s in segments if s._name == "NM1"), Segment.empty())
        else:
            # Backward compatibility: use individual segments
            self._extract_segments([s for s in [nm1] if s and s._name])
            nm1_seg = nm1
        
        # Extract common fields for backward compatibility
        self.payer_name = nm1_seg.element(3)
        self.payer_identifier = nm1_seg.element(9)
        self.payer_identifier_cd = nm1_seg.element(8)
        self.business_entity_type = nm1_seg.element(2)

class PatientIdentity(Identity):
        def __init__(self, segments=None, nm1=Segment.empty(), n3=Segment.empty(), n4=Segment.empty(), dmg=Segment.empty(), pat=Segment.empty(), sbr=Segment.empty(), ref = Segment.empty()):
            # If segments list is provided, use dynamic extraction
            if segments is not None:
                self._extract_segments(segments)
                # Extract common fields for backward compatibility
                nm1_seg = next((s for s in segments if s._name == "NM1"), Segment.empty())
                n3_seg = next((s for s in segments if s._name == "N3"), Segment.empty())
                n4_seg = next((s for s in segments if s._name == "N4"), Segment.empty())
                dmg_seg = next((s for s in segments if s._name == "DMG"), Segment.empty())
                pat_seg = next((s for s in segments if s._name == "PAT"), Segment.empty())
                sbr_seg = next((s for s in segments if s._name == "SBR"), Segment.empty())
                ref_seg = next((s for s in segments if s._name == "REF"), Segment.empty())
            else:
                # Backward compatibility: use individual segments
                self._extract_segments([s for s in [nm1, n3, n4, dmg, pat, sbr, ref] if s and s._name])
                nm1_seg = nm1
                n3_seg = n3
                n4_seg = n4
                dmg_seg = dmg
                pat_seg = pat
                sbr_seg = sbr
                ref_seg = ref
            
            # Extract common fields for backward compatibility
            self.subsciber_identifier = nm1_seg.element(9)
            self.name = ' '.join([nm1_seg.element(3), nm1_seg.element(4), nm1_seg.element(5)])
            self.patient_relationship_cd = pat_seg.element(1)
            self.subscriber_relationship_cd = sbr_seg.element(2)
            self.street = n3_seg.element(1)
            self.city = n4_seg.element(1)
            self.state = n4_seg.element(2)
            self.zip = n4_seg.element(3)
            self.dob = dmg_seg.element(2)
            self.dob_format = dmg_seg.element(1)
            self.gender_cd = dmg_seg.element(3)
            self.mrn = ref_seg.element(2)

class ClaimIdentity(Identity):
    #
    # clm, cl1 are individual segments
    # dtp is a loop of 0 or more dates 
    #
    def __init__(self, segments=None, clm=Segment.empty(), dtp=[], cl1 = Segment.empty(), k3 = Segment.empty(), hi = Segment.empty(), ref = [], amt = [],  principal_hi = Segment.empty(), other_hi = []):
        # If segments list is provided, use dynamic extraction
        if segments is not None:
            self._extract_segments(segments)
            # Extract common fields for backward compatibility
            clm_seg = next((s for s in segments if s._name == "CLM"), Segment.empty())
            dtp_segs = [s for s in segments if s._name == "DTP"]
            cl1_seg = next((s for s in segments if s._name == "CL1"), Segment.empty())
            k3_seg = next((s for s in segments if s._name == "K3"), Segment.empty())
            hi_segs = [s for s in segments if s._name == "HI"]
            ref_segs = [s for s in segments if s._name == "REF"]
            amt_segs = [s for s in segments if s._name == "AMT"]
            principal_hi_seg = next((s for s in hi_segs if s.element(1, 0) == "BBR"), Segment.empty())
            other_hi_segs = [s for s in hi_segs if s.element(1, 0) == "BBQ"]
        else:
            # Backward compatibility: use individual segments
            all_segments = [s for s in [clm, cl1, k3, hi, principal_hi] if s and s._name]
            all_segments.extend(dtp)
            all_segments.extend(ref)
            all_segments.extend(amt)
            all_segments.extend(other_hi)
            self._extract_segments(all_segments)
            clm_seg = clm
            dtp_segs = dtp
            cl1_seg = cl1
            k3_seg = k3
            hi_segs = [hi] if hi and hi._name else []
            ref_segs = ref if isinstance(ref, list) else [ref] if ref and ref._name else []
            amt_segs = amt if isinstance(amt, list) else [amt] if amt and amt._name else []
            principal_hi_seg = principal_hi
            other_hi_segs = other_hi if isinstance(other_hi, list) else [other_hi] if other_hi and other_hi._name else []
        
        # Extract common fields for backward compatibility
        self.claim_id = clm_seg.element(1)
        self.claim_amount = clm_seg.element(2)
        self.facility_type_code = clm_seg.element(5)
        self.claim_dates = [{'date_cd': s.element(1), 'date_format': s.element(2), 'date': s.element(3)} for s in dtp_segs]
        self.admission_type = cl1_seg.element(1)
        self.admission_src_cd = cl1_seg.element(2)
        self.discharge_status_cd = cl1_seg.element(3)
        self.encounter_id = k3_seg.element(1)
        self.drg_cd = hi_segs[0].element(1) if hi_segs and hi_segs[0].element(1).startswith("DR") else ""
        self.clm_refs = [{'id_code_qualifier': x.element(1), 'id': x.element(2)} for x in ref_segs]
        self.other_amts = [{'amt_qualifier_cd': a.element(1), 'amt': a.element(2)} for a in amt_segs]
        self.icd10_pcs_cds = { #only 837i
            'principal_prcdr': {'prcdr_cd': principal_hi_seg.element(1,1), 'date_format': principal_hi_seg.element(1,2), 'date': principal_hi_seg.element(1,3)},
            'other_cds': list(itertools.chain(*[
                [{'prcdr_cd': s.element(i,1), 'date_format': s.element(i,2), 'date': s.element(i,3)} for i in list(range(1, s.segment_len()))]
            for s in other_hi_segs])) 
        }

# POA is the last sub element of the respective segments
class DiagnosisIdentity(Identity):
    def __init__(self, segments=None, hi_segments=None):
        # If segments list is provided, use dynamic extraction
        if segments is not None:
            self._extract_segments(segments)
            hi_segs = [s for s in segments if s._name == "HI"]
        elif hi_segments is not None:
            # Backward compatibility: use hi_segments parameter
            self._extract_segments(hi_segments)
            hi_segs = hi_segments
        else:
            self._extract_segments([])
            hi_segs = []
        
        # Extract common fields for backward compatibility
        self.principal_dx_cd = "" if [s.element(1,1) for s in hi_segs if s.element(1, 0) == "ABK"] == [] else [s.element(1,1) for s in hi_segs if s.element(1, 0) == "ABK"][0]
        self.principal_dx_poa = "" if [s.element(1,1) for s in hi_segs if s.element(1, 0) == "ABK"] == [] else [s.element(1,s.sub_element_len(1)-1) for s in hi_segs if s.element(1,0) == "ABK"][0]
        self.admitting_dx_cd = "" if [s.element(1,1) for s in hi_segs if s.element(1, 0) == "ABJ"] == []	else [s.element(1,1) for s in hi_segs if s.element(1, 0) == "ABJ"][0]
        self.admitting_dx_poa = "" if [s.element(1,1) for s in hi_segs if s.element(1, 0) == "ABJ"] == []	else [s.element(1,s.sub_element_len(1)-1) for s in hi_segs if s.element(1,0) == "ABJ"][0]
        self.reason_visit_dx_cd = "" if [s.element(1,1) for s in hi_segs if s.element(1, 0) == "APR"] == [] else [s.element(1,1) for s in hi_segs if s.element(1, 0) == "APR"][0]
        self.reason_visit_dx_poa = "" if [s.element(1,1) for s in hi_segs if s.element(1, 0) == "APR"] == [] else [s.element(1,s.sub_element_len(1)-1) for s in hi_segs if s.element(1,0) == "APR"][0]
        self.external_injury_dx_cd = "" if [s.element(1,1) for s in hi_segs if s.element(1, 0) == "ABN"] == [] else [s.element(1,1) for s in hi_segs if s.element(1, 0) == "ABN"][0]
        self.external_injury_dx_poa = "" if [s.element(1,1) for s in hi_segs if s.element(1, 0) == "ABN"] == [] else [s.element(1,s.sub_element_len(1)-1) for s in hi_segs if s.element(1,0) == "ABN"][0]
        self.other_dx_cds = list(itertools.chain(*[[{'dx_cd': s.element(i,1), 'poa': s.element(i,s.sub_element_len(i)-1)} for i in list(range(1, s.segment_len())) if s.element(i,0) == "ABF"]
            for s in hi_segs]))
    
class Submitter_Receiver_Identity(Identity):
    def __init__(self, segments=None, nm1=Segment.empty(), per= Segment.empty()):
        # If segments list is provided, use dynamic extraction
        if segments is not None:
            self._extract_segments(segments)
            # Extract common fields for backward compatibility
            nm1_seg = next((s for s in segments if s._name == "NM1"), Segment.empty())
            per_seg = next((s for s in segments if s._name == "PER"), Segment.empty())
        else:
            # Backward compatibility: use individual segments
            self._extract_segments([s for s in [nm1, per] if s and s._name])
            nm1_seg = nm1
            per_seg = per
        
        # Extract common fields for backward compatibility
        self.type = 'Organization' if nm1_seg.element(2) == '2' else 'Individual'
        self.name = nm1_seg.element(3) if self.type == 'Organization' else ' '.join([nm1_seg.element(3), nm1_seg.element(4,dne=""), nm1_seg.element(5)])
        if per_seg.element(0):
            self.sbmtter_contact_name = "" if per_seg.element(2) == '' else per_seg.element(2)
            self.sbmtter_contacts = "" if (per_seg.element(3), per_seg.element(4)) == [] else [(per_seg.element(3), per_seg.element(4)), #telephone
                                                                                    (per_seg.element(5), per_seg.element(6))] # usually fax/email

                
class RemittanceIdentity(Identity):
    """
    Universal identity class for remittance (835) loops.
    Stores all segments dynamically.
    """
    def __init__(self, segments=None):
        if segments is not None:
            self._extract_segments(segments)
        else:
            self._extract_segments([])

class RemittanceClaimIdentity(Identity):
    """
    Universal identity class for remittance claim loops.
    Stores all segments dynamically.
    """
    def __init__(self, segments=None):
        if segments is not None:
            self._extract_segments(segments)
        else:
            self._extract_segments([])

class ServiceLine(Identity):

    def __init__(self, segments=None, **kwargs):
        # Greedy Extraction: Capture EVERYTHING in the loop
        if segments:
            self._extract_segments(segments)
        else:
            self._extract_segments([])
        
        # Specific Mapping: Set convenience attributes (e.g., self.line_chrg_amt)
        for k, v in kwargs.items():
            setattr(self, k, v)

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
    def from_sv2(cls, segments, sv2, lx, dtp, amt):
        # Pass the FULL segment list for greedy extraction
        sl = cls(segments=segments)
        
        # Map standard 837I fields for backward compatibility
        sl.claim_line_number = lx.element(1)
        sl.line_chrg_amt = sv2.element(3)
        sl.units = sv2.element(5)
        sl.units_measurement = sv2.element(4)
        sl.prcdr_cd = sv2.element(2, 1, "")
        sl.prcdr_cd_type = sv2.element(2, 0, "")
        sl.modifier_cds = ','.join(filter(lambda x: x!="", [sv2.element(2, 2, ""), sv2.element(2, 3, ""), sv2.element(2, 4,""), sv2.element(2, 5, "")]))
        sl.revenue_cd = sv2.element(1)
        sl.service_dates = [{'date_cd': s.element(1), 'date_format': s.element(2), 'date': s.element(3)} for s in dtp]
        sl.other_amts = [{'amt_qualifier_cd': a.element(1), 'amt': a.element(2)} for a in amt]
        return sl

    #
    # Professional Claims
    #
    @classmethod
    def from_sv1(cls, segments, sv1, lx, dtp, amt):
        # Pass the FULL segment list for greedy extraction
        sl = cls(segments=segments)
        
        # Map standard 837P fields for backward compatibility
        sl.claim_line_number = lx.element(1)
        sl.line_chrg_amt = sv1.element(2)
        sl.units = sv1.element(4)
        sl.units_measurement = sv1.element(3)
        sl.prcdr_cd = sv1.element(1, 1)
        sl.prcdr_cd_type = sv1.element(1, 0)
        sl.modifier_cds = ','.join(filter(lambda x: x!="", [sv1.element(1, 2, ""), sv1.element(1, 3, ""), sv1.element(1, 4,""), sv1.element(1, 5, "")]))
        sl.place_of_service = sv1.element(5)
        sl.dg_cd_pntr = sv1.element(7)
        sl.service_dates = [{'date_cd': s.element(1), 'date_format': s.element(2), 'date': s.element(3)} for s in dtp]
        sl.other_amts = [{'amt_qualifier_cd': a.element(1), 'amt': a.element(2)} for a in amt]
        return sl

class RemittanceServiceLineIdentity(Identity):
    """
    Universal identity class for remittance (835) service line loops.
    Stores all segments dynamically.
    """
    def __init__(self, segments=None):
        if segments:
            self._extract_segments(segments)
        else:
            self._extract_segments([])
        

        
