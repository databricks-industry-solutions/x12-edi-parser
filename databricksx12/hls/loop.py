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
                },
                'Provider Address Line 1': {
                    'loop': '2000AA',
                    'reference_ids': ('N3', '1')
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
                },
                'Claim ID': {
                    'loop': '2300',
                    'reference_ids': ('CLM', '1')
                },
                'Claim Amount': {
                    'loop': '2300',
                    'reference_ids': ('CLM', '2')
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

        self.sender = self.segments_by_name("GS")[0].element(2)
        self.receiver = self.segments_by_name("GS")[0].element(3)

    def claim_segments(self):
        return [(i, x.element(2)) for i, x in self.segments_by_name_index("CLM")]

    def claim_count(self):
        return len(self.segments_by_name_index("CLM"))

    def get_transaction_info(self, tx_summary, clm_index):
        """
        retrieves transaction information for a claim from hierarchical summary
        Eg., get the transaction range for claim index "x"
        """
        return next((info for _, info in tx_summary.items()
                     if info['index_start'] <= int(clm_index) <= info['index_end']), None)

    def get_ranges(self, tx_info, clm_index, use_children=False):
        """
        extracts numeric ranges for parent and optionally children based on transaction but if children add an index to filter to the right one
        Eg., find ranges for parent and children loops for processing
        """
        if use_children and 'children' in tx_info:
            return [(child['index_start'], child['index_end']) for child in tx_info['children']
                    if child['index_start'] <= int(clm_index) <= child['index_end']]
        else:
            return [(tx_info['index_start'], tx_info['index_end'])]

    def find_elements_based_on_ranges(self, ranges, target_segment_name, target_element_index, secondary_reference=None):
        """
        filters and maps EDI segments to extract required elements based on their position and type
        """
        def process_range(range_tuple): return [
            segment.element(int(target_element_index))
            for segment in self.segments_by_position(range_tuple[0], range_tuple[1])
            if segment.segment_name() == target_segment_name and
            segment.segment_len() > int(target_element_index) and
            (secondary_reference is None or segment.element(
                int(secondary_reference[1])) == secondary_reference[0])
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

        ranges = self.get_ranges(tx_info, clm_index, use_children)
        return self.find_elements_based_on_ranges(ranges, target_segment_name, target_element_index, secondary_reference)

    def find_reference_element(self, clm_segment, loop_key, description=None):
        loop_info = self.loop_mapping.get_mapping(loop_key, description)
        if not loop_info:
            return []

        target_segment_name, target_element_index = loop_info['reference_ids']
        secondary_reference = loop_info.get('secondary_reference', None)
        use_children = loop_key == '22'

        return self.extract_elements_from_claim(clm_segment,
                                                target_segment_name,
                                                target_element_index,
                                                use_children,
                                                secondary_reference)


"""
sample_data_837i_edited = open("/sampledata/837/CHPW_Claimdata_edited.txt", "rb").read().decode("utf-8")
claims = Loop(sample_data_837i_edited)
claims.find_reference_element(claims.claim_segments()[0], '22', 'Claim ID')
Outputs:
['1805080AV3648339']
"""
