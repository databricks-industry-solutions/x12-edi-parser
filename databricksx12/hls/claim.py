from databricksx12.edi import *
from databricksx12.hls import loop
#
# Base claim class
#


class Claim():

    def __init__(self):
        pass

    @staticmethod
    def from_dictionary(d):
        pass


class Claim837i(Claim):

    NAME = "837I"

# Format of 837P https://www.dhs.wisconsin.gov/publications/p0/p00265.pdf


class Claim837p(Claim):

    NAME = "837P"


#
# Base claim builder (transaction -> 1 or more claims)
#

class ClaimBuilder(EDI):
    #
    # Given claim type (837i, 837p, etc), segments, and delim class, build claim level classes
    #
    def __init__(self, trnx_type, trnx_data, delim_cls):
        self.trnx_type = trnx_type
        self.data = trnx_data
        self.delim_cls = delim_cls

        self.loop_summary = loop.Loop(trnx_data)

    #
    # Returns a dictionary of "loop name" : "loop data"
    #

    def build_claim(self, clm_segment):
        return {
            "1000A": {
                "desc": "Submitter Name",
                "segments": self.loop_summary.sender
            },
            "1000B": {
                "desc": "Receiver Name",
                "segments": self.loop_summary.receiver
            },
            "2000A": {
                "desc": "Billing Provider",
                "segments": self.loop_summary.find_reference_element(clm_segment, '20', 'Information Source')
            },
            "2000B": {
                "desc": "Subscriber",
                "segments": self.loop_summary.find_reference_element(clm_segment, '22', 'Subscriber')
            },
            "2010BA": {
                "desc": "Patient",
                "segments": (self.loop_summary.find_reference_element(clm_segment, '22', 'Individual First Name'),
                             self.loop_summary.find_reference_element(clm_segment, '22', 'Individual Last Name'))

            },
            "2010BB": {
                "desc": "Payer",
                "segments": self.loop_summary.find_reference_element(clm_segment, '22', 'Payer Name'),
            },
            "2300": {
                "desc": "Claim",
                "segments": (self.loop_summary.find_reference_element(clm_segment, '22', 'Claim ID'),
                             self.loop_summary.find_reference_element(clm_segment, '22', 'Claim Amount'))
            }
        }

    #
    # Given transaction type, transaction segments, and delim info, build out claims in the transaction
    #  @return a list of Claim for each "clm" segment
    #
    def build(self):
        return [self.build_claim(seg) for seg in self.loop_summary.claim_segments()]


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
