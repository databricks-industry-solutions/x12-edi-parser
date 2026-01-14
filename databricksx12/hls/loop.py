from databricksx12.edi import *
from functools import reduce

class LoopMapping:

    #
    # class to hold k,v of hl_code, loop
    #
    def __init__(self, mappings=None):
        self.mappings = mappings if mappings is not None else {
            '20': {
                'loop name': 'Information Source',
                'loop': '2000A'
            },
            '22': {
                'loop name': 'Subscriber',
                'loop': '2000B'
            },
            '23': {
                'loop name': 'Patient',
                'loop': '2000C'
            },
        }

    #
    # Get hl_code associated with the loop
    #
    def get_hl_code(self, loop):
        return None if (temp := [hl_code for hl_code, v in self.mappings.items() if v['loop'] == loop]) == [] else temp[0]

    def get_mapping(self, element, description=None):
        """ Returns a specific mapping based on element key and description. """
        mappings = self.mappings.get(element, {})
        if description:
            return mappings.get(description, None)
        return None


class Loop(EDI):

    
    def __init__(self, data, delim_cls=AnsiX12Delim, loop_mapping=LoopMapping()):
        self.data = data
        self.format_cls = delim_cls
        self.mapping = loop_mapping
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
    # Get the specified loop based upon a position, else return None if does not exist
    #  @param pos - the position of the data point
    #  @param loop - the loop from the mapping that is being searched for
    #
    #  @return None if not found, otherwise value from loop_hierarchy
    #
    def get_loop(self, pos, loop):
        return None if (temp := self.mapping.get_hl_code(loop)) is None else self.find_hl_codes(pos, temp)

    #
    # same as above, but only returns segment list
    #
    def get_loop_segments(self, pos, loop):
        return [] if (temp := self.get_loop(pos, loop)) is None else self.data[temp['start_idx']:temp['end_idx']]

    #
    # Build a complete hierarchical view of all HL segments start and end positions 
    #
    def build_hierarchy(self):
        """
        Return all start indexes         
        """
        # Optimization: build hierarchy in a single pass.
        # start_indexes are already ordered by position, so end_idx is the next start_idx.
        start_indexes = self._start_indexes
        hierarchy = {}
        for idx, (hl_id, start_idx, parent_id, hl_code, child_code) in enumerate(start_indexes):
            next_start = start_indexes[idx + 1][1] if idx + 1 < len(start_indexes) else len(self.data)
            prev_child_code = start_indexes[idx - 1][4] if idx > 0 else 0
            hierarchy[hl_id] = {
                "start_idx": start_idx,
                "end_idx": next_start,
                "parent_id": parent_id,
                "hl_code": hl_code,
                "child_code": child_code,
                "subordinate_ind": prev_child_code  # true if previous HL04=1
            }
        return hierarchy

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
    def traverse_loops(self, hl_code, loop, visited=None):
        # Prevent infinite recursion by tracking visited loops
        if visited is None:
            visited = set()
        
        # Get a unique identifier for this loop (using start_idx)
        loop_id = loop.get('start_idx')
        
        # Check if we've already visited this loop (circular reference)
        if loop_id in visited:
            return None
        
        visited.add(loop_id)
        
        # Check for depth limit (safety measure)
        if len(visited) > 100:  # Reasonable max depth for HL hierarchies
            return None
        
        if loop['hl_code'] == hl_code:
            return loop 
        elif loop['parent_id'] == "":
            return None
        else:
            parent = self.determine_parent(loop)
            if parent is None:
                return None
            return self.traverse_loops(hl_code, parent, visited)

    #
    # parent is either the parent_id or the previous HL segment if there was a child indicator section
    #
    def determine_parent(self, loop):
        if loop['subordinate_ind'] == 0:
            # Return the loop object directly from hierarchy, not just the ID
            parent_id = loop['parent_id']
            return self.loop_hierarchy.get(parent_id) if parent_id else None
        else:
            # Get the previous HL segment
            prev_hl = self.determine_previous_hl(loop['start_idx'])
            if prev_hl is None:
                return None
            # prev_hl is a tuple: (id, start_idx, parent_id, hl_code, child_code)
            # Extract the ID (first element) and look it up in the hierarchy
            prev_hl_id = prev_hl[0]
            return self.loop_hierarchy.get(prev_hl_id)
        
    #
    #  returns the HL segment 
    #
    def _filter_hl_on_position(self, pos_idx):
        return (list(temp)[0] if (temp := filter(lambda v: v['start_idx'] <= pos_idx < v['end_idx'], self.loop_hierarchy.values())) else None)


    #
    # determine if the HL segment at pos is a subordinate child of a parent
    #  i.e. (parent has child code =1) and parent is previous HL segment 
    #
    #
    def subordinate_child_indicator(self, pos):
        return 0 if self.determine_previous_hl(pos) is None else self.determine_previous_hl(pos)[4]

    #
    # Determine the previous HL segment based upon a position
    #
    def determine_previous_hl(self, pos):
        try:
            return reduce(lambda a,b: a if a[1] > b[1] else b,
                          filter(lambda x: x[1] < pos, self._start_indexes))
        except: 
            return None #when there is no preceding hl segment
        
                
"""
sample_data_837i_edited = open("/sampledata/837/CHPW_Claimdata_edited.txt", "rb").read().decode("utf-8")
claims = Loop(sample_data_837i_edited)
claims.find_reference_element(claims.claim_segments()[0], '22', 'Claim ID')
Outputs:
['1805080AV3648339']
"""
