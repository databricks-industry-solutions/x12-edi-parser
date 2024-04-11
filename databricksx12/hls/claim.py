from databricksx12.edi import *
from databricksx12.hls import hierarchicalloop
#
# Base claim class
#
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

    def get_mapping(self, element):
        return self.mappings.get(element, None)


class Claim(EDI):
    def __init__(self, data, delim_cls=AnsiX12Delim, loop_mapping=LoopMapping(), element=None):
        super().__init__(data, delim_cls)
        self.loop_mapping = loop_mapping
        self.target_element = element
        self.loop_value = self.loop_mapping.get_mapping(
            self.target_element).get('loop')
        self.target_segment_name, self.target_element_index = self.get_reference_names_of_loop(
            self.loop_value)

        # find all CLM segments (important for indexing the last HL or SBR within a tx)
        self.clm_segments = self._clm_identifiers()

    # this feels misplaced; how to fix?
    def get_reference_names_of_loop(self, loop):
        identifiers = {
            '2000A': ('NM1', '3'),
            '2000B': ('SBR', '4'),
        }
        return identifiers.get(loop, (None, None))

    def _clm_identifiers(self):
        # Find the segments where CLM loop begins
        indexed_CLM_segments = self.segments_by_name_index("CLM")
        return [(i, x.element(2)) for i, x in indexed_CLM_segments]

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


class ClaimManager:
    def __init__(self, data, delim_cls=AnsiX12Delim):
        self.hlmanager = hierarchicalloop.HierarchicalLoopManager(data)
        self.hl_summary = self.hlmanager.summary
        # need to sort mapping dependency
        self.claim = Claim(data, delim_cls, LoopMapping(), element='20')
        self.claim_summaries = [{
            'clm_ind': clm_index,
            'parent_counter': self._find_claim_in_tx(self.hl_summary, clm_index)[0],
            'child_start_index': self._find_claim_in_tx(self.hl_summary, clm_index)[1],
            'parent_range': self.get_ranges(self.hl_summary, self._find_claim_in_tx(self.hl_summary, clm_index)[0])[0],
            'children_ranges': self.get_ranges(self.hl_summary, self._find_claim_in_tx(self.hl_summary, clm_index)[0])[1]
        } for clm_index in [i for (i, j) in self.claim.clm_segments]]

    def _find_claim_in_tx(self, tx_summary, clm_index):
        for parent_counter, parent_info in tx_summary.items():
            if parent_info['parent_index_start'] <= int(clm_index) <= parent_info['parent_index_end']:
                for child_info in parent_info['children']:
                    if child_info['child_index_start'] <= int(clm_index) <= child_info['child_index_stop']:
                        return parent_counter, child_info['child_index_start']
        return None, None

    def get_ranges(self, hl_summary_dict, loop_counter):
        info = hl_summary_dict.get(loop_counter, None)
        if info is None:
            return None

        parent_range = (info['parent_index_start'], info['parent_index_end'])
        children_ranges = [(child['child_index_start'], child['child_index_stop'])
                           for child in info.get('children', [])]

        return parent_range, children_ranges

    def _find_billing_providers(self):
        first_lines = []
        for summary in self.claim_summaries:
            lines = self.claim.extract_lines_based_on_ranges(
                [summary['parent_range']
                 ], self.claim.target_segment_name, self.claim.target_element_index
            )
            if lines:  # Check if any lines were extracted
                # Append the first line of this iteration
                first_lines.append(lines[0])
        return first_lines

    def _find_subscribers(self):
        pass


"""
claims = ClaimManager(sample_data_chpw_claimdata)
claims.claim_summaries

[{'clm_ind': 23,
  'parent_counter': '1',
  'child_start_index': 16,
  'parent_range': (7, 35),
  'children_ranges': [(16, 35)]},
 {'clm_ind': 57,
  'parent_counter': '63',
  'child_start_index': 50,
  'parent_range': (41, 69),
  'children_ranges': [(50, 69)]},
 {'clm_ind': 91,
  'parent_counter': '49',
  'child_start_index': 84,
  'parent_range': (75, 103),
  'children_ranges': [(84, 103)]},
 {'clm_ind': 125,
  'parent_counter': '75',
  'child_start_index': 118,
  'parent_range': (109, 138),
  'children_ranges': [(118, 138)]},
 {'clm_ind': 160,
  'parent_counter': '79',
  'child_start_index': 153,
  'parent_range': (144, 172),
  'children_ranges': [(153, 172)]}]

  claims._find_billing_providers()
  ['BH CLINIC OF VANCOUVER',
 'BH CLINIC OF VANCOUVER',
 'BH CLINIC OF VANCOUVER',
 'BH CLINIC OF VANCOUVER',
 'BH CLINIC OF VANCOUVER']
"""


class Claim837i(Claim):

    NAME = "837I"

# Format of 837P https://www.dhs.wisconsin.gov/publications/p0/p00265.pdf


class Claim837p(Claim):

    NAME = "837P"
