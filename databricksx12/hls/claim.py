from databricksx12.edi import *

#
# Base claim class
#
class Claim(EDI):

    def __init__(self, segments, delim_cls = AnsiX12Delim):
        self.data = segments
        self.format_cls = delim_cls
        #For Raven TODO marked
        self.claim_identifier = None #TODO include both CH and RP values here 
        self.claim_lines = None #TODO Maintain a list of claim lines using ClaimLine class
        self.subscriber = None  #TODO selecting the subscriber
        self.patient = None     #TODO selecting the patient info, maybe patient should be its own class? 


    #
    # TODO total amount billed at the header of the claim
    #
    def header_total_billed_amount(self):
        pass

    #
    # TODO total amount billed across lines
    #
    def lines_total_billed_amount(self):
        pass 


    
class ClaimLine(Segment):

    #
    # TODO build out claim line uses (case class)
    #
    def __init__(self):
        pass
        """
        select fields: 
          procedure code
          procedure code type (HCPCS, CPT4, ICD10)
          revenuce code
          procedure modifier codes
          billed amount 
          
        """


class Claim837i(Claim):

    NAME = "837I"

#Format of 837P https://www.dhs.wisconsin.gov/publications/p0/p00265.pdf
class Claim837p(Claim):

    NAME = "837P"

    
