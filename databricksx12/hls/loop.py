from databricksx12.edi import *
from functools import reduce

class LoopMapping:
    def __init__(self, mappings=None):
        self.mappings = mappings if mappings is not None else {
            '20': {
                'loop name': 'Information Source',
                'loop': '2000A'
            },
            '22': {
                'loop name': 'Subscriber',
                'loop': '2000B'
            }
        }

    def get_mapping(self, element, description=None):
        """ Returns a specific mapping based on element key and description. """
        mappings = self.mappings.get(element, {})
        if description:
            return mappings.get(description, None)
        return None


class Loop(EDI):

    
    def __init__(self, data, delim_cls=AnsiX12Delim, loop_mapping=LoopMapping()):
        super().__init__(data, delim_cls)
        self.loop_mapping = loop_mapping
        self._start_indexes = self._build_hierarchy_start_indexes()
        self.loop_hierarchy = self.build_hierarchy()
        """
        loop_hierarchy = { unique_id : {
            start_idx : ""
            end_idx : ""
            parent_id : ""
            hl_code : ""
            child_code: ""
           }
        }
        """

    #
    # Build a complete hierarchical view of all HL segments start and end positions 
    #
    def build_hierarchy(self):
        """
        Return all start indexes         
        """
        return {
            x[0]: {
                "start_idx": x[1],
                "end_idx": self._determine_end_index(x[1]),
                "parent_id": x[2],
                "hl_code": x[3],
                "child_code": x[4]
            }
            for x in self._start_indexes
        }

    #
    # Return a tuple of all HL segments, start index, id, parent id, child code, and hl_code
    #
    def _build_hierarchy_start_indexes(self):
        return [ ( x.element(1), #id
                   i, # "start_idx"
                   x.element(2), # "parent_id"
                   x.element(3), # "hl_code"
                   x.element(4))  # "child_code"
         for i,x in self.segments_by_name_index("HL")]

    #
    # Determine the end index of an HL segment
    #  @param start_idx - the start index of the existing HL segment
    #  x[1] = start index from tuple in _build_hierarchy_start_indexes
    #
    def _determine_end_index(self, start_idx):
        return min([x[1] for x in self._start_indexes if x[1] > start_idx] + [len(self.data)])

    #
    # Primary search function within HL
    #   @param pos_idx - the reference point
    #   @param hl_code - the hl code being searched for
    #
    #   @returns None if not found, otherwise the value from loop_hierarchy
    def find_hl_codes(self, pos_idx, hl_code):
        init_hl = self._filter_hl_on_position(pos_idx)
        return (None if init_hl is None else self.traverse_loops(hl_code, init_hl))


    #
    # Go from child to parent searching for the specified hl_code
    #
    def traverse_loops(self, hl_code, loop):
        if loop['hl_code'] == hl_code:
            return loop 
        elif loop['parent_id'] == "":
            return None
        else:
            return self.traverse_loops(hl_code, self.loop_hierarchy.get(loop['parent_id']))

    #
    # 
    #
    def _filter_hl_on_position(self, pos_idx):
        return (list(temp)[0] if (temp := filter(lambda v: v['start_idx'] <= pos_idx <= v['end_idx'], self.loop_hierarchy.values())) else None)

        
    #
    # Will only ever return one element or None
    #
    def _fitler_hl_on_position_and_code(self, pos_idx, hl_code):
        return (list(temp)[0] if (temp := filter(lambda v: v['hl_code'] == hl_code and v['start_idx'] <= pos_idx <= v['end_idx'] ,self.loop_hierarchy.values())) else None) 

                
"""
sample_data_837i_edited = open("/sampledata/837/CHPW_Claimdata_edited.txt", "rb").read().decode("utf-8")
claims = Loop(sample_data_837i_edited)
claims.find_reference_element(claims.claim_segments()[0], '22', 'Claim ID')
Outputs:
['1805080AV3648339']
"""
