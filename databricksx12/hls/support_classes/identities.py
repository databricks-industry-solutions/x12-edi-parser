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
                    self.type = 'Entity' if segment.element(2) == '2' else 'Individual'
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
                self.id_code = segment.element(1) # submitter's identifier
                self.claim_amount = segment.element(2)
                if segment.element(5).split(':')[1] == 'B':
                    self.facility_code = 'Outpatient Hospital' if segment.element(3).split(':')[0]== 22 else 'Other'
                