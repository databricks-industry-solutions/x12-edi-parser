from databricksx12.edi import Segment
from typing import List, Dict

from collections import defaultdict
from functools import reduce

class Identity:

    # entity name identities associated with every NM1 line. a combination may occur within loops
    nm1_identifiers = {
        '85': 'Billing Provider',  # entity that is billing for the services provided and 87 disregarded
        'IL': 'Insured',           # insured individual
        'QC': 'Patient',           # patient for 837P and PAT segments in 837i
        '82': 'Rendering Provider',# individual or group that performed the service
        'DN': 'Referring Provider',# doctor who referred the patient to another doctor
        '77': 'Service Facility',  # location where the service was performed
        'DQ': 'Supervising Provider', # provider who oversees the patient's care
        '71': 'Attending Provider',# provider with primary responsibility for the patient at the time of service
        'DK': 'Ordering Provider', # provider who ordered the service or item
        'PR': 'Payer',             # insurance company or payer
        'PE': 'Payee',             # entity receiving the payment

    }
        
    def __init__(self, segments: List[Segment]):
        self.name: str = None
        self.street: str = None
        self.type: str = None
        self.provider_type: str = None
        self.city: str = None
        self.state: str = None
        self.zip: str = None
        self.id: str = None
        self.npi: str = None
        self.build(segments)

    # build entity and address for any identity
    def build(self, loop: List[Segment]):
        nm1_segment = next(filter(lambda segment: segment.element(0) == 'NM1' and segment.segment_len() >= 10, loop), None)
        n3_segment = next(filter(lambda segment: segment.element(0) == 'N3', loop), None) # taking only the first address lines
        n4_segment = next(filter(lambda segment: segment.element(0) == 'N4', loop), None)

        list(map(self.process_nm1_segment, [nm1_segment] if nm1_segment else []))
        list(map(self.process_n3_segment, [n3_segment] if n3_segment else []))
        list(map(self.process_n4_segment, [n4_segment] if n4_segment else []))
    
    def process_nm1_segment(self, segment: Segment):
        self.type = 'Organization' if segment.element(2) == '2' else 'Individual'
        self.name = segment.element(3) if self.type == 'Organization' else ' '.join([segment.element(3), segment.element(4), segment.element(5)])
        self.entity_type = self.nm1_identifiers.get(segment.element(1), 'Unknown')
        self.npi = segment.element(9) if len(segment.element(9)) == 10 else None
        self.id = segment.element(9) if len(segment.element(9)) != 10 else None

    def process_n3_segment(self, segment: Segment):
        self.street = segment.element(1)

    def process_n4_segment(self, segment: Segment):
        self.city = segment.element(1)
        self.state = segment.element(2)
        self.zip = segment.element(3)

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}
    


class BillingIdentity(Identity):
    def __init__(self, billing_segments: List[Segment]):
        super().__init__(billing_segments)
        list(map(lambda segment: Identity([segment]).to_dict(), billing_segments))
        

class SubscriberIdentity(Identity):
    def __init__(self, subscriber_segments: List[Segment]):
        self.relationship_to_insured = None
        super().__init__(subscriber_segments)
        self.build_subscriber(subscriber_segments)

    def build_subscriber(self, subscriber_loop: List[Segment]):
        sbr_segment = next(filter(lambda segment: segment.element(0) == 'SBR', subscriber_loop), None)
        if sbr_segment:
            self.relationship_to_insured = 'Self' if sbr_segment.element(2) == '18' else 'Dependent'




class PatientIdentity(Identity):
        def __init__(self, patient_segments: List[Segment]):
            super().__init__(patient_segments)
            self.build_patient(patient_segments)

        def build_patient(self, patient_loop: List[Segment]):
            def process_patient_segment(segment: Segment):
                self.type = 'Patient'
                self.name = ' '.join([segment.element(3), segment.element(4), segment.element(5)])
            return list(map(process_patient_segment, filter(lambda s: s.element(0) == 'NM1' and s.element(1) == 'QC', patient_loop)))
        

class ClaimIdentity(Identity):
    def __init__(self, claim_segments: List[Segment]):
        self.patient_id = None
        self.claim_amount = None
        self.facility_type_code = None
        self.claim_code_freq = None
        self.admission_date = None
        self.benefits_assign_flag = None
        self.claim_id = None
        self.admission_type = None # only 837I?

        self.pricipal_diagnosis_code = None

        self.providers = defaultdict(list) # still need?
        super().__init__(claim_segments)
        self.build_claim_lines(claim_segments)

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
        # if principle_diagnosis_segment:
        #     self.process_principal_diagnosis_segment(principle_diagnosis_segment)
    
        # Process other diagnosis codes
        # self.other_diagnosis_codes = [
        #     code for segment in other_diagnosis_segments
        #     for i, code in enumerate(segment.element(1).split(':'))
        #     if i % 2 != 0
        # ]
        

        # Process NM1 segments for providers
        nm1_segments = filter(lambda segment: segment.element(0) == 'NM1', claim_loop)
        list(map(lambda segment: self.providers[self.nm1_identifiers.get(segment.element(1))].append(Identity([segment]).to_dict()), nm1_segments))

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
        super().__init__(submitter_segments)
        self.build_submitter_lines(submitter_segments)
    
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
        super().__init__(receiver_segments)
        self.build_receiver_lines(receiver_segments)

    def build_receiver_lines(self, receiver_loop: List[Segment]):
        nm1_segments = filter(lambda segment: segment.element(0) == 'NM1' and segment.element(1) == '40', receiver_loop)
        return list(map(self.process_nm1_segment, nm1_segments))


                
class ServiceLine(Identity):

    def common(self, sv, lx, dtp):
        self.claim_line_number = lx.element(1)
        self.service_date = dtp.element(3)
        self.service_time = dtp.element(1)
        self.service_date_format = dtp.element(2)

    #
    # Institutional Claims
    # 
    def __init__(self, sv2, lx, dtp):
        self.common(sv2, lx, dtp)
        self.units = sv2.element(6)
        self.units_measurement = sv1.element(5)
        self.line_chrg_amt = sv2.element(4)
        self.prcdr_cd = sv2.element(2, 1)
        self.prcdr_cd_type = sv2.element(2, 0)
        self.modifier_cds = ','.join(filter(lambda x: x!="", [sv1.element(2, 2, ""), sv1.element(2, 3, ""), sv1.element(2, 4, ""), sv1.element(2, 5, "")]))
        self.revenue_cd = sv2.element(1)

    #
    # Professional Claims
    #
    def __init__(self, sv1, lx, dtp):
        self.common(sv1, lx, dtp)
        self.units = sv1.element(4)
        self.units_measurement = sv1.element(3)
        self.line_chrg_amt = sv1.element(2)
        self.prcdr_cd = sv1.element(1, 1)
        self.prcdr_cd_type = sv1.element(1, 0)
        self.modifier_cds = ','.join(filter(lambda x: x!="", [sv1.element(1, 2, ""), sv1.element(1, 3, ""), sv1.element(1, 4, ""), sv1.element(1, 5, "")]))
        self.place_of_service = sv1.element(5)
        self.dg_cd_pntr = sv1.element(7)

        
