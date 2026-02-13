from databricksx12.hls.claim import MedicalClaim
import functools


class Benefit(MedicalClaim):

    NAME = "270"
    def __init__(self):
        #TODO 1 define input loops for a 270. 
        #self.trx_header_loop = trx_header_loop
        #self.payer_loop = payer_loop
        #self.payee_loop = payee_loop
        #self.clm_loop = clm_loop
        self.build()

    def build(self):
        pass
        #TODO 2a define how data is processed out of the loops into key/value pairs
        #self.trx_header_info = self.populate_trx_loop()
        #self.payer_info = self.populate_payer_loop()
        

    #TODO 2b helper functions to parse information
    def populate_trx_loop(self):
        #nm1=self._first([x for x in self.claim_loop if x.element(1) == "82"],"NM1")
        pass

    def to_json(self):
        return {
            #TODO 3 define how the output grouping of information should present in json format
#            **{'payment': self.trx_header_info},
#            **{'payer': self.payer_info},
            **{'input_loop_segments': self.loop_segments_info} #the input loops

        }
    
    
