from databricksx12.edi import *
from databricksx12.hls.loop import *
import itertools


#
# Base claim class
#


class MedicalClaim(EDI):

    def __init__(self,
                 sender_loop = [],
                 receiver_loop = [],
                 billing_loop = [],
                 subscriber_loop = [],
                 patient_loop = [],
                 claim_loop =  [],
                 sl_loop = [] #service line loop
                 ):
        self.sender_loop = sender_loop
        self.receiver_loop = receiver_loop
        self.billing_loop = billing_loop
        self.subscriber_loop = subscriber_loop
        self.patient_loop = patient_loop
        self.claim_loop = claim_loop
        self.sl_loop = sl_loop
        
        self.build()

    def billing_loop(self):
        return {
            "billing_prvdr_name": "TODO",
            "billing_npi": "TODO",
            "billing_street_address": "TODO",
            "billing_zip_cd": "TODO",
            "billing_state_cd": "TODO"
            }
    
    def subscriber_loop(self):
        return {
            "TODO": "TODO"
            }

    #
    #
    #
    def patient_loop(self):
        #Note - if this doesn't exist then its the same as subscriber loop
        return {
            "TODO": "TODO"
            }
    
    def toJson(self):
        {
            **self.patient_loop(),
            **self.subscriber_loop(),
            **self.billing_loop()
         }


    #not sure if this should be here or not, but you get the idea
    def build():
        self.billing_info = self.billing_loop()
        self.subscriber_info = self.subscriber_loop()
        self.patient_info = self.subscriber_loop() if self.patient_loop = [] else self.patient_loop()
        


class Claim837i(MedicalClaim):

    NAME = "837I"


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
            sender_loop = [],
            receiver_loop = [],
            billing_loop = self.loop.get_loop_segments(idx, "2000A"),
            subscriber_loop = self.loop.get_loop_segments(idx, "2000B"),
            patient_loop = self.loop.get_loop_segments(idx, "2000C"),
            claim_loop =  [],
            sl_loop = [] #service line loop
        )

    #
    # Given transaction type, transaction segments, and delim info, build out claims in the transaction
    #  @return a list of Claim for each "clm" segment
    #
    def build(self):
        return [self.build_claim(seg, i) for i, seg in self.segments_by_name_index("CLM")]

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
