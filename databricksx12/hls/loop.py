from databricksx12.edi import *
from functools import reduce
from databricksx12.hls import hierarchicalloop

class LoopMapping:
    def __init__(self, mappings=None):
        self.mappings = mappings if mappings is not None else {
            '20': {
                'Information Source': {
                    'loop': '2000A',
                    'reference_ids': ('NM1', '3'),
                    'secondary_reference': ('85', '1') 
                }
            },
            '22': {
                'Subscriber': {
                    'loop': '2000B',
                    'reference_ids': ('SBR', '4')
                },
                'Individual First Name': {
                    'loop': '2010BA',
                    'reference_ids': ('NM1', '4'), 
                    'secondary_reference': ('IL', '1') 
                },
                'Individual Last Name': {
                    'loop': '2010BA',
                    'reference_ids': ('NM1', '3'), 
                    'secondary_reference': ('IL', '1') 
                },
                'Payer Name': {
                    'loop': '2010BB',
                    'reference_ids': ('NM1', '3'), 
                    'secondary_reference': ('PR', '1') 
                }
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
        self.hlmanager = hierarchicalloop.HierarchicalLoopManager(data)
        self.hl_summary = self.hlmanager.summary
        self.clm_segments = [(i, x.element(2)) for i, x in self.segments_by_name_index("CLM")]
    
    def get_transaction_info(self, tx_summary, clm_index):
        """
        retrieves transaction information for a claim from hierarchical summary
        Eg., Getting the transaction range for claim index "x"
        """
        return next((info for _, info in tx_summary.items()
                     if info['index_start'] <= int(clm_index) <= info['index_end']), None)

    def get_ranges(self, tx_info, use_children=False):
        """
        extracts numeric ranges for parent and optionally children based on transaction
        Eg., find ranges for parent and children loops for processing
        """
        parent_range = (tx_info['index_start'], tx_info['index_end'])
        if use_children:
            return [(child['index_start'], child['index_end']) for child in tx_info.get('children', [])]
        return [parent_range]


    def find_elements_based_on_ranges(self, ranges, target_segment_name, target_element_index, secondary_reference=None):
        """
        filters and maps EDI segments to extract required elements based on their position and type
        """
        process_range = lambda range_tuple: [
            segment.element(int(target_element_index))
            for segment in self.segments_by_position(range_tuple[0], range_tuple[1])
            if segment.segment_name() == target_segment_name and 
            segment.segment_len() > int(target_element_index) and
            (secondary_reference is None or segment.element(int(secondary_reference[1])) == secondary_reference[0])
        ]
        return reduce(lambda acc, lst: acc + lst, map(process_range, ranges), [])

    def extract_elements_from_claim(self, clm_segment, target_segment_name, target_element_index, use_children=False, secondary_reference=None):
        """
        a higher-level function that ties together the previous functions to get tx info, the ranges of interest, and elements from every range
        """
        clm_index = clm_segment[0]
        tx_info = self.get_transaction_info(self.hl_summary, clm_index)
        if not tx_info:
            return None

        ranges = self.get_ranges(tx_info, use_children)
        return self.find_elements_based_on_ranges(ranges, target_segment_name, target_element_index, secondary_reference)


    def find_reference_elements(self, loop_key, description=None):
        """
        extract reference elements from claims based on loop mapping and hierarchy and handles children separately if specified by loop key.
        Eg., find billing provider names under loop '20' from an EDI transaction.
        """

        loop_info = self.loop_mapping.get_mapping(loop_key, description)
        if not loop_info:
            return []
        
        target_segment_name, target_element_index = loop_info['reference_ids']
        secondary_reference = loop_info.get('secondary_reference', None)

        use_children = loop_key == '22'
        process_clm_segment = lambda clm_segment: self.extract_elements_from_claim(clm_segment, 
                                                                                   target_segment_name, 
                                                                                   target_element_index, 
                                                                                   use_children,
                                                                                   secondary_reference)
        reference_list = list(filter(None, map(process_clm_segment, self.clm_segments)))
        
        return [summary for summary in reference_list] 


"""
sample_data_837i_edited = open("/sampledata/837/CHPW_Claimdata_edited.txt", "rb").read().decode("utf-8")
claims = Loop(sample_data_837i_edited)
claims.find_reference_elements('20', 'Information Source')
Outputs:
[['BH CLINIC OF VANCOUVER'],
 ['BH CLINIC OF VANCOUVER'],
 ['BH CLINIC OF VANCOUVER'],
 ['BH CLINIC OF VANCOUVER'],
 ['BH CLINIC OF VANCOUVER']]
"""
"""
claims.find_reference_elements('22', 'Payer Name')
Outputs:
[['COMMUNITY HEALTH PLAN OF WASHINGTON'],
 ['COMMUNITY HEALTH PLAN OF WASHINGTON'],
 ['COMMUNITY HEALTH PLAN OF WASHINGTON'],
 ['COMMUNITY HEALTH PLAN OF WASHINGTON'],
 ['COMMUNITY HEALTH PLAN OF WASHINGTON',
  'COMMUNITY HEALTH PLAN OF WASHINGTON',
  'COMMUNITY HEALTH PLAN OF WASHINGTON']]
"""
"""
claims.find_reference_elements('22', 'Individual First Name')
Outputs:
[['JOHN'], ['SUSAN'], ['JOHN'], ['SUSAN'], ['JOHN', 'JOHN', 'JOHN']]
  """