from databricksx12.edi import Segment
from typing import List


class Identity:
    def __init__(self, segments: List[Segment]):
        self.name: str = None
        self.street: str = None
        self.type: str = None
        self.city: str = None
        self.state: str = None
        self.zip: str = None
        self.build(segments)

    def build(self, loop: List[Segment]):
        for segment in loop:
            if segment.element(0) == 'N3':
                self.street = segment.element(1)
            elif segment.element(0) == 'N4':
                self.city = segment.element(1)
                self.state = segment.element(2)
                self.zip = segment.element(3)

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}


class BillingIdentity(Identity):
    def __init__(self, billing_segments: List[Segment]):
        super().__init__(billing_segments)
        self.npi = None
        self.build_billing(billing_segments)

    def build_billing(self, billing_loop: List[Segment]):
        for segment in billing_loop:
            if segment.element(0) == 'NM1':
                if segment.element(1) == '85':      # Hardcoded to 85 for Billing Providers
                    self.type = 'Organization' if segment.element(2) == '2' else 'Individual'
                    self.name = segment.element(3) if self.type == 'Organization' else ' '.join([segment.element(3), segment.element(4), segment.element(5)])
                    self.npi = segment.element(9)


class SubscriberIdentity(Identity):
    def __init__(self, subscriber_segments: List[Segment]):
        super().__init__(subscriber_segments)
        self.id_code = None
        self.relationship_to_insured = None
        self.build_subscriber(subscriber_segments)

    def build_subscriber(self, subscriber_loop: List[Segment]):
        for segment in subscriber_loop:
            if segment.element(0) == 'NM1':
                if segment.element(1) == 'IL':      # Hardcoded to IL for Insured
                    self.type = 'Organization' if segment.element(2) == '2' else 'Individual'
                    self.name = segment.element(3) if self.type == 'Organization' else ' '.join([segment.element(3), segment.element(4), segment.element(5)])
                    self.id_code = segment.element(9)
            elif segment.element(0) == 'SBR':
                self.relationship_to_insured = 'Self' if segment.element(2) == '18' else 'Dependent'     # information about subscriber/dependent 01 = Spouse; 18 = Self; 19 = Child; G8 = Other


class PatientIdentity(Identity):
    def __init__(self, patient_segments: List[Segment]):
        super().__init__(patient_segments)
        self.build_patient(patient_segments)

    def build_patient(self, patient_loop: List[Segment]):
        for segment in patient_loop:
            if segment.element(0) == 'NM1':
                if segment.element(1) == 'QC':      # Hardcoded to QC for Patient
                    self.type = 'Patient'
                    self.name = ' '.join([segment.element(3), segment.element(4), segment.element(5)])


class ClaimIdentity(Identity):
    def __init__(self, claim_segments: List[Segment]):
        super().__init__(claim_segments)
        self.id_code = None
        self.facility_code = None
        self.claim_amount = None
        self.build_claim_lines(claim_segments)

    def build_claim_lines(self, claim_loop: List[Segment]):
        for segment in claim_loop:
            if segment.element(0) == 'CLM':
                # TODO Inst/Prof
                self.id_code = segment.element(1) # submitter's identifier
                self.claim_amount = segment.element(2)
                if segment.element(5).split(':')[1] == 'B': # professional claims
                    self.facility_code = 'Outpatient Hospital' if segment.element(5).split(':')[0]== 22 else 'Other'
            # TODO: additional provider lines?



class SubmitterIdentity(Identity):
    def __init__(self, submitter_segments: List[Segment]):
        super().__init__(submitter_segments)
        self.tax_id = None
        self.contact_name = None
        self.contacts = []
        self.build_submitter_lines(submitter_segments)

    def build_submitter_lines(self, submitter_loop: List[Segment]):
        for segment in submitter_loop:
            if segment.element(0) == 'NM1'and segment.element(1) == '41':
                self.process_nm1_segment(segment)
            elif segment.element(0) == 'PER':
                self.process_per_segment(segment)
    
    def process_nm1_segment(self, segment):
        self.type = 'Organization' if segment.element(2) == '2' else 'Individual'
        self.name = segment.element(3) if self.type == 'Organization' else ' '.join([segment.element(3), segment.element(4), segment.element(5)])
        self.tax_id = segment.element(9) # id

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
        
        self.contacts.append(contact)


class ReceiverIdentity(Identity):
    def __init__(self, receiver_segments: List[Segment]):
        super().__init__(receiver_segments)
        self.id_code = None
        self.build_receiver_lines(receiver_segments)

    def build_receiver_lines(self, receiver_loop: List[Segment]):
        for segment in receiver_loop:
            if segment.element(0) == 'NM1' and segment.element(1) == '40':
                self.type = 'Organization' if segment.element(2) == '2' else 'Individual'
                self.name = segment.element(3) if self.type == 'Organization' else ' '.join([segment.element(3), segment.element(4), segment.element(5)])
                self.id_code = segment.element(9) # id


class ServiceIdentity(Identity):
    def __init__(self, sl_segments: List[Segment]):
            super().__init__(sl_segments)
            self.services = {
                'Professional': [],
                'Institutional': []
            }
            self.build_sl_lines(sl_segments)

    def build_sl_lines(self, sl_loop: List[Segment]):
        for segment in sl_loop:
            if segment.element(0) == 'SV1':  # Professional service
                service = self.parse_professional_service(segment)
                self.services['Professional'].append(service)
            elif segment.element(0) == 'SV2':  # Institutional service
                service = self.parse_institutional_service(segment)
                self.services['Institutional'].append(service)

    def parse_professional_service(self, segment: Segment):
        service_type, procedure_code = segment.element(1).split(':')[0:2] #assuming 7 elements but choosing first two
        return {
            'Type of service/claim': 'Professional',
            'Type': service_type,
            'Procedure Code': procedure_code,
            'Procedure Amount': segment.element(2)
        }

    def parse_institutional_service(self, segment: Segment):
        revenue_code = segment.element(1)
        service_type, procedure_code = segment.element(2).split(':')[0:2] #assuming 7 elements but choosing first two
        return {
            'Type of service/claim': 'Institutional',
            'Revenue Code': revenue_code,
            'Type': service_type,
            'Procedure Code': procedure_code,
            'Procedure Amount': segment.element(3)
        }


                

