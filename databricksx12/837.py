from databricksx12.edi import *


class Claim837(EDI):

    #
    # Returns the total number of transactions using trailer segment 
    #
    def num_transactions(self):
        return len(self.segments_by_name("SE"))

    #
    # Return all segments associated with each transaction
    #  [ trx1[SEGMENT1, ... SEGMENTN], trx2[SEGMENT1, ... SEGMENTN] ... ]
    #
    def transaction_segments(self):
        [self.segments_by_position(i - x.get_element(1),i) for i,x in self.segments_by_name_index("SE")] 

    
