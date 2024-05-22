from databricksx12.edi import EDI, AnsiX12Delim, Segment
from databricksx12.hls.loop import Loop
from databricksx12.hls.support_classes.identities import *
from typing import List, Dict
from collections import defaultdict
from functools import reduce


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
    def _populate_patient_loop(self) -> Dict[str, str]:
        # Note - if this doesn't exist then its the same as subscriber loop
        # Note to include in loop: information about subscriber/dependent relationship is marked by Element 2
        # 01 = Spouse; 18 = Self; 19 = Child; G8 = Other
        return PatientIdentity(self.patient_loop)
    
    def _populate_claim_loop(self) -> Dict[str, str]:
        return ClaimIdentity(self.claim_loop)

    #
    #
    #
    def _populate_grouped_entities(self, loop: List[Segment]) -> Dict[str, List[Dict[str, str]]]:
        # if we want a list of NM1 entities belonging within a loop 
        def group_segments_by_provider(loop, nm1_identifiers: dict = Identity.nm1_identifiers) -> Dict[str, List[List[Segment]]]:
            def reducer(acc, segment):
                provider_type, grouped = acc
                if segment.element(0) == 'NM1':
                    provider_type = nm1_identifiers.get(segment.element(1))
                    if provider_type:
                        grouped[provider_type] = grouped.get(provider_type, []) + [[segment]]
                elif provider_type:
                    grouped[provider_type][-1] += [segment]
                return provider_type, grouped
        
            _, grouped = reduce(reducer, loop, (None, {}))
            return grouped
        
        return {
            provider_type: [Identity(segments).to_dict() for segments in group]
            for provider_type, group in group_segments_by_provider(loop).items()
        }
    

   

    """
    Overall Asks
    - Coordination of Benefits flag -- > self.benefits_assign_flag in Claim Identity
    - Patient / Subscriber same person flag --> self.relationship_to_insured in Suscriber in Claim Identity

    Claim needs
    - principal ICD10 diagnosis code
    - other ICD10 diagnosis codes as an array 
    - hcfa place of service -- segment.element(5).split(':')?
    - claim id? - done
    - admission type code - only in 837i?
    - facility type code - done
    - claim frequency code - done

    Claim line needs
    - This should return an array 
    
    Servicing provider needs
    - TBD
    """
    def to_json(self):
        return {
            **{'submitter': self.submitter_info.to_dict()},
            **{'reciever': self.receiver_info.to_dict()},
            **{'subscriber': self.subscriber_info.to_dict()},
            **{'patient': self.patient_info.to_dict()},
            **{'providers': [{"TODO":"TODO"}]},
            **{'claim_header': self.claim_info.to_dict()},
            **{'claim_lines': [x.to_dict() for x in self.sl_info]}, #List 
            **{'grouped_subscriber_entities': self.subscriber_entities_info}, # call for all entities in a loop[]
        }

    #
    # Returns each claim line as an array of segments that make up the claim line
    #
    def claim_lines(self):
        return list(map(lambda i: self.sl_loop[i[0]:i[1]],
                self._index_to_tuples([(i) for i,y in enumerate(self.sl_loop) if y.segment_name()=="LX"]+[len(self.sl_loop)])))

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
        self.sl_info =  self._populate_sl_loop()

        self.claim_entities_info = self._populate_grouped_entities(self.claim_loop)
        self.subscriber_entities_info = self._populate_grouped_entities(self.subscriber_loop)


class Claim837i(MedicalClaim):

    NAME = "837I"

    # Format of 837P https://www.dhs.wisconsin.gov/publications/p0/p00265.pdf

    def _populate_sl_loop(self, missing=""):
        return list(
            map(lambda s:
                ServiceLine(
                    sv2=[x for x in s if x.segment_name()=="SV2"][0],
                    lx=[x for x in s if x.segment_name()=="LX"][0],
                    dtp=[x for x in s if x.segment_name()=="DTP"][0]
                ),self.claim_lines()))

class Claim837p(MedicalClaim):

    NAME = "837P"
    
    def _populate_sl_loop(self, missing=""):
        return list(
            map(lambda s:
                ServiceLine(
                    sv1=[x for x in s if x.segment_name()=="SV1"][0],
                    lx=[x for x in s if x.segment_name()=="LX"][0],
                    dtp=[x for x in s if x.segment_name()=="DTP"][0]
                ), self.claim_lines()))


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
            sender_receiver_loop=self.get_submitter_receiver_loop(idx),
            billing_loop=self.loop.get_loop_segments(idx, "2000A"),
            subscriber_loop=self.loop.get_loop_segments(idx, "2000B"),
            patient_loop=self.loop.get_loop_segments(idx, "2000C"),
            claim_loop=self.get_claim_loop(idx),
            sl_loop=self.get_service_line_loop(idx),  # service line loop
        )

    #
    # Determine claim loop: starts at the clm index and ends at LX segment, or CLM segment, or end of data
    #
    def get_claim_loop(self, clm_idx):
        sl_start_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("LX"))))
        clm_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("CLM"))))

        if sl_start_indexes:
            clm_end_idx = min(sl_start_indexes)
        elif clm_indexes:
            clm_end_idx = min(clm_indexes + [len(self.data)])
        else:
            clm_end_idx = len(self.data)
        
        return self.data[clm_idx:clm_end_idx]

    #
    # fetch the indices of LX and CLM segments that are beyond the current clm index
    #
    def get_service_line_loop(self, clm_idx):
        sl_start_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("LX"))))
        tx_end_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] > clm_idx, self.segments_by_name_index("SE"))))
        if sl_start_indexes:
            sl_end_idx = min(tx_end_indexes + [len(self.data)])
            return self.data[min(sl_start_indexes):sl_end_idx]
        return []

    def get_submitter_receiver_loop(self, clm_idx):
        bht_start_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] < clm_idx, self.segments_by_name_index("BHT"))))
        bht_end_indexes = list(map(lambda x: x[0], filter(lambda x: x[0] < clm_idx and x[1].element(3) == '20', self.segments_by_name_index("HL"))))
        if bht_start_indexes:
            sub_rec_start_idx = max(bht_start_indexes)
            sub_rec_end_idx = max(bht_end_indexes)

            return self.data[sub_rec_start_idx:sub_rec_end_idx]
        return []


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
