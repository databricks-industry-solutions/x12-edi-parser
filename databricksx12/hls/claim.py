from databricksx12.edi import EDI, AnsiX12Delim
from databricksx12.hls.loop import Loop
from databricksx12.hls.support_classes.identities import BillingIdentity, SubscriberIdentity, PatientIdentity, ClaimIdentity, SubmitterIdentity, ReceiverIdentity, ServiceIdentity
from typing import List, Dict


#
# Base claim class
#

class MedicalClaim(EDI):

    def __init__(
        self,
        sender_receiver_loop: List = [],
        billing_loop: List = [],
        subscriber_loop: List = [],
        patient_loop: List = [],
        claim_loop: List = [],
        sl_loop: List = [], 
    ):
        self.sender_receiver_loop = sender_receiver_loop # extracted together
        self.billing_loop = billing_loop
        self.subscriber_loop = subscriber_loop
        self.patient_loop = patient_loop
        self.claim_loop = claim_loop
        self.sl_loop = sl_loop

        self.build()

    def _populate_submitter_loop(self) -> Dict[str, str]:
        return SubmitterIdentity(self.sender_receiver_loop)
    
    def _populate_receiver_loop(self) -> Dict[str, str]:
        return ReceiverIdentity(self.sender_receiver_loop)
    
    def _populate_billing_loop(self) -> Dict[str, str]:
        return BillingIdentity(self.billing_loop)

    def _populate_subscriber_loop(self) -> Dict[str, str]:
        return SubscriberIdentity(self.subscriber_loop)

    #
    #
    #
    def _populate_patient_loop(self) -> Dict[str, str]:
        # Note - if this doesn't exist then its the same as subscriber loop
        # Note to include in loop: information about subscriber/dependent relationship is marked by Element 2
        # 01 = Spouse; 18 = Self; 19 = Child; G8 = Other
        return PatientIdentity(self.patient_loop)
    
    def _populate_claim_loop(self) -> Dict[str, str]:
        return ClaimIdentity(self.claim_loop)

    def _populate_sl_loop(self) -> Dict[str, str]:
        return ServiceIdentity(self.sl_loop)
    

    def toJson(self):
        {**self.sender_receiver_loop(), **self.claim_loop(), **self.patient_loop(), **self.subscriber_loop(), **self.billing_loop()}

    # not sure if this should be here or not, but you get the idea
    def build(self) -> None:
        self.submitter_info = self._populate_submitter_loop()
        self.receiver_info = self._populate_receiver_loop()
        self.billing_info = self._populate_billing_loop()
        self.subscriber_info = self._populate_subscriber_loop()
        self.patient_info = (
            self._populate_subscriber_loop() if self.patient_loop == [] else self._populate_patient_loop()
        )
        self.claim_info = self._populate_claim_loop()
        self.sl_info = self._populate_sl_loop()



class Claim837i(MedicalClaim):

    NAME = "837I"
    # sender / receiver ?

# Format of 837P https://www.dhs.wisconsin.gov/publications/p0/p00265.pdf


class Claim837p(MedicalClaim):

    NAME = "837P"


class Claim835(MedicalClaim):

    NAME = "835"


#
# Base claim builder (transaction -> 1 or more claims)
#


class ClaimBuilder(EDI):
    #
    # Given claim type (837i, 837p, etc), segments, and delim class, build claim level classes
    #
    def __init__(self, trnx_type_cls, trnx_data, delim_cls=AnsiX12Delim):
        self.data = trnx_data
        self.format_cls = delim_cls
        self.trnx_cls = trnx_type_cls
        self.loop = Loop(trnx_data)

    #
    # Builds a claim object from
    #
    # @param clm_segment - the claim segment of claim to build
    # @param idx - the index of the claim segment in the data
    #
    #  @return the clas containing the relevent claim information
    #
    def build_claim(self, clm_segment, idx):
        return self.trnx_cls(
            sender_receiver_loop=self.loop.get_submitter_receiver_loop(idx),
            billing_loop=self.loop.get_loop_segments(idx, "2000A"),
            subscriber_loop=self.loop.get_loop_segments(idx, "2000B"),
            patient_loop=self.loop.get_loop_segments(idx, "2000C"),
            claim_loop=self.loop.get_claim_loop(idx),
            sl_loop=self.loop.get_service_line_loop(idx),  # service line loop
        )

    #
    # Given transaction type, transaction segments, and delim info, build out claims in the transaction
    #  @return a list of Claim for each "clm" segment
    #
    def build(self):
        return [
            self.build_claim(seg, i) for i, seg in self.segments_by_name_index("CLM")
        ]


"""
sample_data_837i_edited = open("/sampledata/837/CHPW_Claimdata_edited.txt", "rb").read().decode("utf-8")
claim_class = ClaimBuilder(trnx_type='837I', trnx_data=sample_data_837i_edited, delim_cls=AnsiX12Delim)
claim_class.build()

[{'1000A': {'desc': 'Submitter Name', 'segments': 'CLEARINGHOUSE'},
  '1000B': {'desc': 'Receiver Name', 'segments': '123456789'},
  '2000A': {'desc': 'Billing Provider',
   'segments': ['BH CLINIC OF VANCOUVER']},
  '2000B': {'desc': 'Subscriber', 'segments': ['COMMUNITY HLTH PLAN OF WASH']},
  '2010BA': {'desc': 'Patient', 'segments': (['JOHN'], ['SUBSCRIBER'])},
  '2010BB': {'desc': 'Payer',
   'segments': ['COMMUNITY HEALTH PLAN OF WASHINGTON']},
  '2300': {'desc': 'Claim', 'segments': (['1805080AV3648339'], ['20'])}},
 {'1000A': {'desc': 'Submitter Name', 'segments': 'CLEARINGHOUSE'},
  '1000B': {'desc': 'Receiver Name', 'segments': '123456789'},
  '2000A': {'desc': 'Billing Provider',
   'segments': ['BH CLINIC OF VANCOUVER']},
  '2000B': {'desc': 'Subscriber', 'segments': ['COMMUNITY HLTH PLAN OF WASH']},
  '2010BA': {'desc': 'Patient', 'segments': (['SUSAN'], ['PATIENT'])},
  '2010BB': {'desc': 'Payer',
   'segments': ['COMMUNITY HEALTH PLAN OF WASHINGTON']},
  '2300': {'desc': 'Claim', 'segments': (['1805080AV3648347'], ['50.1'])}},
 {'1000A': {'desc': 'Submitter Name', 'segments': 'CLEARINGHOUSE'},
  '1000B': {'desc': 'Receiver Name', 'segments': '123456789'},
  '2000A': {'desc': 'Billing Provider',
   'segments': ['BH CLINIC OF VANCOUVER']},
  '2000B': {'desc': 'Subscriber', 'segments': ['COMMUNITY HLTH PLAN OF WASH']},
  '2010BA': {'desc': 'Patient', 'segments': (['JOHN'], ['SUBSCRIBER'])},
  '2010BB': {'desc': 'Payer',
   'segments': ['COMMUNITY HEALTH PLAN OF WASHINGTON']},
  '2300': {'desc': 'Claim', 'segments': (['1805080AV3648340'], ['11.64'])}},
 {'1000A': {'desc': 'Submitter Name', 'segments': 'CLEARINGHOUSE'},
  '1000B': {'desc': 'Receiver Name', 'segments': '123456789'},
  '2000A': {'desc': 'Billing Provider',
   'segments': ['BH CLINIC OF VANCOUVER']},
  '2000B': {'desc': 'Subscriber', 'segments': ['COMMUNITY HLTH PLAN OF WASH']},
  '2010BA': {'desc': 'Patient', 'segments': (['SUSAN'], ['PATIENT'])},
  '2010BB': {'desc': 'Payer',
   'segments': ['COMMUNITY HEALTH PLAN OF WASHINGTON']},
  '2300': {'desc': 'Claim', 'segments': (['1805080AV3648353'], ['234'])}},
 {'1000A': {'desc': 'Submitter Name', 'segments': 'CLEARINGHOUSE'},
  '1000B': {'desc': 'Receiver Name', 'segments': '123456789'},
  '2000A': {'desc': 'Billing Provider',
   'segments': ['BH CLINIC OF VANCOUVER']},
  '2000B': {'desc': 'Subscriber', 'segments': ['COMMUNITY HLTH PLAN OF WASH']},
  '2010BA': {'desc': 'Patient',
   'segments': (['JOHN', 'JOHN'], ['SUBSCRIBER', 'SUBSCRIBER'])},
  '2010BB': {'desc': 'Payer',
   'segments': ['COMMUNITY HEALTH PLAN OF MASS',
    'COMMUNITY HEALTH PLAN OF WASHINGTON']},
  '2300': {'desc': 'Claim', 'segments': (['1805080AV3648355'], ['20'])}}]
"""
