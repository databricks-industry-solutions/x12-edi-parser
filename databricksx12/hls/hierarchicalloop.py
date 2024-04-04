from databricksx12.edi import *

class LoopMapping:
    def __init__(self):
        self.mappings = {
            '2000A': ('20', 'NM1', '3'),
            '2000B': ('22', 'SBR', '4'),
        }

    def get_identifiers(self, loop_type):
        return self.mappings.get(loop_type, (None, None))


class HierarchicalLoop(EDI):
    def __init__(self, data, delim_cls=AnsiX12Delim, Loop='2000B'):
        super().__init__(data, delim_cls)
        self.loop_mapping = LoopMapping()
        self.target_element, self.target_segment_name, self.target_element_index = self.loop_mapping.get_identifiers(
            Loop)

        # find all HL segments along with the 3rd element that denotes 2000A or 2000B
        self.hl_segments = self._hl_identifiers()

        # find all HL segments along with the 3rd element that denotes 2000A or 2000B
        self.clm_segments = self._clm_identifiers()

        # Calculate ranges and then extract 2000A lines based on those ranges
        self.ranges = self.select_range_of_interest(
            self.hl_segments, self.clm_segments, self.target_element)
        self.extracted_lines = self.extract_lines_based_on_ranges(
            self.ranges, self.target_segment_name, self.target_element_index)

    def _hl_identifiers(self):
        # Find the segments where HL loop begins
        indexed_HL_segments = self.segments_by_name_index("HL")
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
