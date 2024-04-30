from databricksx12.edi import *

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

        pass #self.hl = ??? TODO 
        
    def claim_count():
        return len(self.segments_by_name("CLM"))

    #
    # Returns a dictionary of "loop name" : "loop data" 
    #
    def build_claim(self, clm_index, clm_segment):
        return {
            "1000A": {
                "desc": "Submitter Name",
                "segments": "TODO"
            },
            "1000B": {
                "desc": "Reciever Name",
                "segments": "TODO"
            },
            "2000A": {
                "desc": "Billing Provider HL Level"
                "segments": "TODO"
            },
            "2000B": {
                "desc": "Subscriber HL Level",
                "segments": "TODO"
            },
            "2000C" : {
                "desc": "Patient HL Level",
                "segments": "TODO"
            },
            "2300": {
                "desc": "Claim Information",
                "segments": "TODO"
            }
        }
    
    #
    # Given transaction type, transaction segments, and delim info, build out claims in the transaction
    #  @return a list of Claim for each "clm" segment 
    #
    def build(self):
        return [self.build_claim(i, x) for i,x in segments_by_name_index("CLM")]
    
