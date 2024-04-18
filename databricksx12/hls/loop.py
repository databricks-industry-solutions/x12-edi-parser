from databricksx12.edi import *
from databricksx12.hls import hierarchicalloop

class LoopMapping:
    def __init__(self, mappings=None):
        self.mappings = (mappings if mappings is not None else {
            '20': {
                'description': 'Information Source',
                'loop': '2000A',
                'reference_ids': ('NM1', '3'), ## might delete and use elsewhere
                },
            '22': {
                'description': 'Subscriber',
                'loop': '2000B',
                'reference_ids': ('SBR', '4'), 
                }
        })
    
    def get_mapping(self, element):
        return self.mappings.get(element, None)


class Loop(EDI):
    def __init__(self, data, delim_cls=AnsiX12Delim, loop_mapping=LoopMapping()):
        super().__init__(data, delim_cls)
        self.loop_mapping = loop_mapping
        self.hlmanager = hierarchicalloop.HierarchicalLoopManager(data)
        self.hl_summary = self.hlmanager.summary
        self.clm_segments = self._clm_identifiers()

    def _clm_identifiers(self):
        return [(i, x.element(2)) for i, x in self.segments_by_name_index("CLM")]
    
    #
    # returns element of interest from a range based on first element and index from the line
    #
    def _find_elements_based_on_ranges(self, ranges, target_value, target_index):
        def process_range(range_tuple):
            start, end = range_tuple
            segments_in_range = self.segments_by_position(start, end) # find segments within range
            return list(map(
                lambda segment: segment.element(int(target_index)),
                filter(
                    lambda segment: segment.segment_name() == target_value and 
                                    segment.segment_len() > int(target_index),
                                    segments_in_range
                )
            ))
        return functools.reduce(
            lambda acc, lst: acc + lst, map(process_range, ranges),[]) # map to apply processing to each range and flatten 
    
    #
    # if a claim index (str), returns the parent counter and child_start_index of Tx from the Hierarchical loop summary
    #
    def _find_tx_from_clm(self, tx_summary, clm_index):
        try:
            return next(
                (parent_counter, child['child_index_start'])
                for parent_counter, parent_info in tx_summary.items()
                if parent_info['parent_index_start'] <= int(clm_index) <= parent_info['parent_index_end']
                for child in parent_info['children']
                if child['child_index_start'] <= int(clm_index) <= child['child_index_stop']
            )
        except StopIteration:
            return None, None
    
    #
    # if a loop counter (str), returns the parent and children ranges (tuple) from the Tx of interest from the Hierarchical loop summary
    #
    def _get_ranges(self, tx_summary, loop_counter):
        info = tx_summary.get(loop_counter, None)
        if info is None:
            return None 
        parent_range = (info['parent_index_start'], info['parent_index_end'])
        children_ranges = [(child['child_index_start'], child['child_index_stop']) for child in info.get('children', [])]
        return parent_range, children_ranges

    #
    # filters a claim's tx segment to extract its reference elements
    #
    def _get_elements_from_claim(self, clm_segment, target_segment_name, target_element_index, use_children=False):
        clm_index = clm_segment[0]
        parent_counter, child_start_index = self._find_tx_from_clm(self.hl_summary, clm_index)
        if not parent_counter:
            return None
        parent_range, children_range = self._get_ranges(self.hl_summary, parent_counter)
        return self._find_elements_based_on_ranges([parent_range], target_segment_name, target_element_index)
    
    #
    # map to apply the find_element function over all claim segments based on choice of loop
    #
    def find_reference_elements(self, loop_key):
        loop_info = self.loop_mapping.get_mapping(loop_key)
        if not loop_info:
            return []
        target_segment_name, target_element_index = loop_info['reference_ids']
        use_children = loop_key == '22'  # Use children ranges for '22'
        process_clm_segment = lambda clm_segment: self._get_elements_from_claim(clm_segment, target_segment_name, target_element_index, use_children)
        reference_list = list(filter(None, map(process_clm_segment, self.clm_segments)))
        return [summary[0] for summary in reference_list] if reference_list else [] # only first element or it generalizes to all segments in the range


"""
sample_data_837i_edited = open("/sampledata/837/CHPW_Claimdata_edited.txt", "rb").read().decode("utf-8")
claims = Loop(sample_data_837i_edited)
claims.find_reference_elements('20')
Outputs:
['BH CLINIC OF VANCOUVER',
 'BH CLINIC OF VANCOUVER',
 'BH CLINIC OF VANCOUVER',
 'BH CLINIC OF VANCOUVER',
 'BH CLINIC OF VANCOUVER']
"""
"""
claims.find_reference_elements('22')
Outputs:
['COMMUNITY HLTH PLAN OF WASH',
 'COMMUNITY HLTH PLAN OF WASH',
 'COMMUNITY HLTH PLAN OF WASH',
 'COMMUNITY HLTH PLAN OF WASH',
 'COMMUNITY HLTH PLAN OF WASH']
"""