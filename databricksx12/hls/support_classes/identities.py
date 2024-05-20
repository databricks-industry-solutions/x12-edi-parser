from databricksx12.edi import Segment
from typing import List, Dict

from collections import defaultdict
from functools import reduce

class Identity:

    # provider name identities associated with every NM1 line. a combination may occur within loops
    nm1_identifiers = {
        '85': 'Billing Provider',  # entity that is billing for the services provided
        '87': 'Pay-to Provider',   # entity to which payments are to be sent
        'PR': 'Payer',             # insurance company or payer
        'IL': 'Insured',           # insured individual
        'QC': 'Patient',           # patient
        '82': 'Rendering Provider',# individual or group that performed the service
        'DN': 'Referring Provider',# doctor who referred the patient to another doctor
        '77': 'Service Facility',  # location where the service was performed
        'DQ': 'Supervising Provider', # provider who oversees the patient's care
        '71': 'Attending Provider',# provider with primary responsibility for the patient at the time of service
        'DK': 'Ordering Provider', # provider who ordered the service or item
        'PE': 'Payee',             # entity receiving the payment
    }
        
    def __init__(self, segments: List[Segment]):
        self.name: str = None
        self.street: str = None
        self.type: str = None
        self.city: str = None
        self.state: str = None
        self.zip: str = None
        self.id: str = None
        self.npi: str = None
        self.build(segments)

    # build name and address for any identity
    def build(self, loop: List[Segment]):
        nm1_segments = filter(lambda segment: segment.element(0) == 'NM1' and segment.segment_len() >= 10, loop)
        n3_segments = filter(lambda segment: segment.element(0) == 'N3', loop)
        n4_segments = filter(lambda segment: segment.element(0) == 'N4', loop)

        list(map(self.process_nm1_segment, nm1_segments))
        list(map(self.process_n3_segment, n3_segments))
        list(map(self.process_n4_segment, n4_segments))
    
    def process_nm1_segment(self, segment: Segment):
        self.type = 'Organization' if segment.element(2) == '2' else 'Individual'
        self.name = segment.element(3) if self.type == 'Organization' else ' '.join([segment.element(3), segment.element(4), segment.element(5)])
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
    
    
    @staticmethod
    def group_segments_by_provider(loop: List[Segment], nm1_identifiers: dict) -> Dict[str, List[List[Segment]]]:
        def reducer(acc, segment):
            provider_type, grouped = acc
            if segment.element(0) == 'NM1':
                provider_type = nm1_identifiers.get(segment.element(1))
                if provider_type:
                    grouped[provider_type].append([segment])
            elif provider_type:
                grouped[provider_type][-1].append(segment)
            return provider_type, grouped
        
        _, grouped = reduce(reducer, loop, (None, defaultdict(list)))
        return grouped


class BillingIdentity(Identity):
    def __init__(self, billing_segments: List[Segment]):
        self.providers = defaultdict(list)
        super().__init__(billing_segments)
        self.build_billing(billing_segments)

    def build_billing(self, billing_loop: List[Segment]):
        grouped_segments = self.group_segments_by_provider(billing_loop, self.nm1_identifiers)
        self.providers = defaultdict(list, {
            provider_type: [Identity(segments).to_dict() for segments in group]
            for provider_type, group in grouped_segments.items()
        })
    

    #TODO class pay_to()

class SubscriberIdentity(Identity):
    def __init__(self, subscriber_segments: List[Segment]):
        self.subscribers = defaultdict(list)
        self.relationship_to_insured = None
        super().__init__(subscriber_segments)
        self.build_subscriber(subscriber_segments)

    def build_subscriber(self, subscriber_loop: List[Segment]):
        grouped_segments = self.group_segments_by_provider(subscriber_loop, self.nm1_identifiers)
        self.subscribers = defaultdict(list, {
            subscriber_type: [Identity(segments).to_dict() for segments in group]
            for subscriber_type, group in grouped_segments.items()
        })
    


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
        self.date = None
        self.providers = defaultdict(list)
        super().__init__(claim_segments)
        self.build_claim_lines(claim_segments)

    def build_claim_lines(self, claim_loop: List[Segment]):
        def process_claim_segment(segment: Segment):
            if segment.element(0) == 'CLM':
                self.patient_id = segment.element(1)  # submitter's identifier
                self.claim_amount = segment.element(2)
                codes = segment.element(5).split(':') # codes[1] == A for institutional and B for professional
                self.facility_type_code = codes[0]
                self.claim_code_freq = codes[2]

            if segment.element(0) == 'DTP':
                self.date = segment.element(3)  # format D8:CCYYMMDD

        # process claim-specific segments
        list(map(process_claim_segment, claim_loop))

        # process NM1 segments for providers
        nm1_segments = filter(lambda segment: segment.element(0) == 'NM1', claim_loop)

        # append instead of extend for single items
        list(map(lambda segment: self.providers[self.nm1_identifiers.get(segment.element(1))].append(Identity([segment]).to_dict()), nm1_segments))



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


                
class ServiceIdentity(Identity):
    def __init__(self, sl_segments: List[Segment]):
        self.services = defaultdict(list)
        super().__init__(sl_segments)
        self.build_sl_lines(sl_segments)

    def build_sl_lines(self, sl_loop: List[Segment]):
        sv1_segments = filter(lambda segment: segment.element(0) == 'SV1', sl_loop)
        sv2_segments = filter(lambda segment: segment.element(0) == 'SV2', sl_loop)
        self.services['Professional'] = [self.parse_professional_service(segment) for segment in sv1_segments]
        self.services['Institutional'] = [self.parse_institutional_service(segment) for segment in sv2_segments]


    def parse_professional_service(self, segment: Segment):
        service_type, procedure_code = segment.element(1).split(':')[0:2]  # assuming 7 elements but choosing first two
        return {
            'Type': service_type,
            'Procedure Code': procedure_code,
            'Procedure Amount': segment.element(2)
        }

    def parse_institutional_service(self, segment: Segment):
        service_type, procedure_code = segment.element(2).split(':')[0:2]  # assuming 7 elements but choosing first two
        return {
            'Type': service_type,
            'Revenue Code': segment.element(1),
            'Procedure Code': procedure_code,
            'Procedure Amount': segment.element(3)
        }