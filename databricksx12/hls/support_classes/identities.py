from databricksx12.edi import Segment
from typing import List, Dict

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
        

class SubscriberIdentity(Identity):
    def __init__(self, subscriber_segments: List[Segment]):
        pass
        #self.relationship_to_insured = None
        #super().__init__(subscriber_segments)
        #self.build_subscriber(subscriber_segments)

    def build_subscriber(self, subscriber_loop: List[Segment]):
        sbr_segment = next(filter(lambda segment: segment.element(0) == 'SBR', subscriber_loop), None)
        if sbr_segment:
            self.relationship_to_insured = 'Self' if sbr_segment.element(2) == '18' else 'Dependent'




class PatientIdentity(Identity):
        def __init__(self, patient_segments: List[Segment]):
            pass
            #self.build_patient(patient_segments)

        def build_patient(self, patient_loop: List[Segment]):
            def process_patient_segment(segment: Segment):
                self.type = 'Patient'
                self.name = ' '.join([segment.element(3), segment.element(4), segment.element(5)])
            return list(map(process_patient_segment, filter(lambda s: s.element(0) == 'NM1' and s.element(1) == 'QC', patient_loop)))

class ClaimIdentity(Identity):
    def __init__(self, clm, dtp, cl1 = Segment.empty()):
        self.claim_id = clm.element(1)
        self.claim_amount = clm.element(2)
        self.facility_type_code = clm.element(5)
        self.service_date =dtp.element(3)
        self.date_format = dtp.element(2)
        self.service_time =dtp.element(1)
        self.admission_type = cl1.element(1)
        self.admission_src_cd = cl1.element(2)
        self.discharge_status_cd = cl1.element(3)
        

class DiagnosisIdentity(Identity):
    def __init__(self, hi_segments):
        self.principal_dx_cd = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABK"] == [] else [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABK"][0]
        self.admitting_dx_cd = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABJ"] == []	else [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABJ"][0]
        self.reason_visit_dx_cd = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "APR"] == [] else [s.element(1,1) for s in hi_segments if s.element(1, 0) == "APR"][0]
        self.external_injury_dx_cd = "" if [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABN"] == [] else [s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABN"][0]
        self.other_dx_cds = ",".join([s.element(1,1) for s in hi_segments if s.element(1, 0) == "ABF"])
    
class ClaimIdentity2(Identity):

    def build_claim_lines(self, claim_loop: List[Segment]):
        # Process claim-specific segments
        clm_segments = filter(lambda segment: segment.element(0) == 'CLM', claim_loop)
        dtp_segments = filter(lambda segment: segment.element(0) == 'DTP', claim_loop)
        cli_segments = filter(lambda segment: segment.element(0) == 'CLI', claim_loop)
        ref_segments = filter(lambda segment: segment.element(0) == 'REF' and segment.element(1) == 'D9', claim_loop)
        
        # get only the first HI segment for the pricipal diagnosis code
        principle_diagnosis_segment = filter(lambda segment: segment.element(0) == 'HI' and segment.element(1).split(':')[0] in ['ABK', 'BK'], claim_loop)
        # get all other HI segments for other diagnosis codes
        other_diagnosis_segments = filter(lambda segment: segment.element(0) == 'HI' and segment.element(1).split(':')[0] in ['ABF', 'BF'], claim_loop)


        list(map(self.process_clm_segment, clm_segments))
        list(map(self.process_dtp_segment, dtp_segments))
        list(map(self.process_cli_segment, cli_segments))
        list(map(self.process_ref_segment, ref_segments))

    def process_clm_segment(self, segment: Segment):
        self.patient_id = segment.element(1)  # submitter's identifier
        self.claim_amount = segment.element(2)
        self.benefits_assign_flag = 'Yes' if segment.element(8) == 'Y' else 'No'  # Benefits flag

        place_of_service = segment.element(5).split(':')  # codes[1] == A for institutional and B for professional
        self.facility_type_code = place_of_service[0]
        self.claim_code_freq = place_of_service[2]

    def process_dtp_segment(self, segment: Segment):
        self.date = segment.element(3)  # format D8:CCYYMMDD

    def process_cli_segment(self, segment: Segment):
        self.admission_date = segment.element(1)  # Only in 837I

    def process_ref_segment(self, segment: Segment):
        self.claim_id = segment.element(2)

    # def process_principal_diagnosis_segment(self, segment: Segment):
    #     self.principal_diagnosis_code = segment.element(2)  # assuming HI segment's first element is the principal diagnosis code




class SubmitterIdentity(Identity):
    def __init__(self, submitter_segments: List[Segment]):
        self.contact_name = None
        self.contacts = defaultdict(list)
        #super().__init__(submitter_segments)
        #self.build_submitter_lines(submitter_segments)
    
    def build_submitter_lines(self, submitter_loop: List[Segment]):
        nm1_segments = filter(lambda segment: segment.element(0) == 'NM1' and segment.element(1) == '41', submitter_loop)
        per_segments = filter(lambda segment: segment.element(0) == 'PER', submitter_loop)

        list(map(self.process_nm1_segment, nm1_segments))
        list(map(self.process_per_segment, per_segments))

    def process_per_segment(self, segment):
        self.contact_name = segment.element(2)
        contact_methods = {
            'EM': 'Email',
            'TE': 'Telephone',
            'FX': 'Fax'
        }
        contact = {
            'contact_method': contact_methods.get(segment.element(3), 'Unknown method'),
            'contact_number': segment.element(4)
            }
        # Add additional contact details if present
        if segment.element(5) in contact_methods:
            contact['contact_method_2'] = contact_methods.get(segment.element(5), 'Unknown method')
            contact['contact_number_2'] = segment.element(6)
        
        if segment.element(7) in contact_methods:
            contact['contact_method_3'] = contact_methods.get(segment.element(7), 'Unknown method')
            contact['contact_number_3'] = segment.element(8)
        
        self.contacts['primary'].append(contact)



class ReceiverIdentity(Identity):
    def __init__(self, receiver_segments: List[Segment]):
        pass
        #super().__init__(receiver_segments)
        #self.build_receiver_lines(receiver_segments)

    def build_receiver_lines(self, receiver_loop: List[Segment]):
        nm1_segments = filter(lambda segment: segment.element(0) == 'NM1' and segment.element(1) == '40', receiver_loop)
        return list(map(self.process_nm1_segment, nm1_segments))


                
class ServiceLine(Identity):

    def __init__(self, d):
        for k,v in d.items():
            setattr(self,k,v)

    @staticmethod
    def common(sv, lx, dtp):
        return {
            "claim_line_number": lx.element(1),
            "service_date": dtp.element(3),
            "service_time": dtp.element(1),
            "service_date_format": dtp.element(2)
        }

    #
    # Institutional Claims
    #
    @classmethod
    def from_sv2(cls, sv2, lx, dtp):
        return cls({**cls.common(sv2, lx, dtp),
                    **{
                        "units": sv2.element(5),
                        "units_measurement": sv2.element(4),
                        "line_chrg_amt": sv2.element(3),
                        "prcdr_cd": sv2.element(2, 1, ""),
                        "prcdr_cd_type": sv2.element(2, 0, ""),
                        "modifier_cds": ','.join(filter(lambda x: x!="", [sv2.element(2, 2, ""), sv2.element(2, 3, ""), sv2.element(2, 4,""), sv2.element(2, 5, "")])),
                        "revenue_cd": sv2.element(1)
                    }
                })

    #
    # Professional Claims
    #
    @classmethod
    def from_sv1(cls, sv1, lx, dtp):
        return cls({**cls.common(sv1, lx, dtp),
                    **{
                        "units": sv1.element(4),
                        "units_measurement": sv1.element(3),
                        "line_chrg_amt": sv1.element(2),
                        "prcdr_cd": sv1.element(1, 1),
                        "prcdr_cd_type": sv1.element(1, 0),
                        "modifier_cds": ','.join(filter(lambda x: x!="", [sv1.element(1, 2, ""), sv1.element(1, 3, ""), sv1.element(1, 4,""), sv1.element(1, 5, "")])),
                        "place_of_service": sv1.element(5),
                        "dg_cd_pntr": sv1.element(7)
                    }
                })
        

        
