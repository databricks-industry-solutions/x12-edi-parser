from databricksx12.edi import *

class LoopMapping:

    def __init__(self, mappings=None):
        self.mappings = (mappings if mappings is not None else {
            '20': {
                'description': 'Information Source',
                'loop': '2000A'
                },
            '22': {
                'description': 'Subscriber',
                'loop': '2000B'
                }
            })


   
        
    """
    def __init__(self):
        self.mappings = {
            '2000A': ('20', 'NM1', '3'),
            '2000B': ('22', 'SBR', '4'),
        }


    ADZ want our key = (lookup value found in data), value = additional info needed 
    """


class HierarchicalLoop(EDI):
    def __init__(self, data, delim_cls=AnsiX12Delim, loop_mapping= LoopMapping.mappings):
        super().__init__(data, delim_cls)
        self.loop_mapping = loop_mapping
        self.target_element, self.target_segment_name, self.target_element_index = self.loop_mapping.get_identifiers(
            Loop)

        # find all HL segments along with the 3rd element that denotes 2000A (20) or 2000B (22)
        self.hl_segments = self._hl_identifiers()

        # find all CLM segments (important for indexing the last HL or SBR within a tx)
        self.clm_segments = self._clm_identifiers()

        # Calculate ranges and then extract 2000A/B lines based on those ranges
        self.ranges = self.select_range_of_interest(
            self.hl_segments, self.clm_segments, self.target_element)
        self.extracted_lines = self.extract_lines_based_on_ranges(
            self.ranges, self.target_segment_name, self.target_element_index)

    def _hl_identifiers(self):
        # Find the segments where HL loop begins
        indexed_HL_segments = self.segments_by_nloop_object.extracted_linesame_index("HL")
        return [(i, x.element(3)) for i, x in indexed_HL_segments]

    def _clm_identifiers(self):
        # Find the segments where CLM loop begins
        indexed_CLM_segments = self.segments_by_name_index("CLM")
        return [(i, x.element(2)) for i, x in indexed_CLM_segments]

    def select_range_of_interest(self, hl_indexes, clm_indexes, target_value):
        ranges = []
        start_index = None
        last_index = None

        for index, value in hl_indexes:
            if value == target_value:
                if start_index is not None:
                    ranges.append((start_index+1, index))
                start_index = index
            elif start_index is not None:
                ranges.append((start_index+1, index))
                start_index = None
        if clm_indexes:
            last_index = clm_indexes[-1][0]
        if last_index and start_index is not None:
            ranges.append((start_index+1, last_index))
        return ranges

    def extract_lines_based_on_ranges(self, ranges, target_value, target_index):
        extracted_elements = []
        # Iterate through each range in the list
        for start, end in ranges:
            # Retrieve the segments within this range
            segments_in_range = self.segments_by_position(start, end)

            desired_elements = map(
                lambda segment: segment.element(int(target_index)),
                filter(
                    lambda segment: segment.segment_name() == target_value and len(
                        segment.data.split(segment.format_cls.ELEMENT_DELIM)) > int(target_index),
                    segments_in_range
                )
            )
            extracted_elements.extend(desired_elements)

        return list(extracted_elements)


    def parent_loops(self):
        pass

    def child_loops(self, parent_loop_num):
        pass

    """
       @return
         -index of each HL segment
         -index of parent segments
         -be able to answer "where is loop XYZ?" and "at this location, what looop am i in?"
    """
    def _hl_segment_indexes(self):
        pass


    self.hl_parents = {
        parent:
        { index_start : value
          index_end : value
           children : [
               hl_child : {
                   index_start: value
                   index_end: value
                   }
               ]
        }


    self.hl = HL()
    self.claim_start_index = segment(clm)

    Who is my billing provider?  hl.get_loop(20)
    Who is my subscriber?
    Who is my patient? 
    
    
