from databricksx12.edi import *


class HierarchicalLoop(EDI):
    def __init__(self, data, delim_cls=AnsiX12Delim):
        super().__init__(data, delim_cls)

        # find all HL and SE segments to find start and end of loops + CLM segments
        self.indexed_HL_segments = self.segments_by_name_index("HL")
        self.indexed_SE_segments = self.segments_by_name_index("SE")

        # parent and children loops
        self.parent_loops = self._parent_loops()
        self.child_loops = self._child_loops(self.parent_loops)
        self.subchild_loops = self._child_loops(self.child_loops)

    def _parent_loops(self):
        parent_start_loops = []
        for i, segment in self.indexed_HL_segments:
            # Check if the second element is empty (and the third element is '20' and the last element is '1')
            if segment.element(2) == '':
                # index of parent, counter, and if child
                parent_start_loops.append(
                    (i, segment.element(1), segment.element(-1)))

        parent_end_loops = [i for i, x in self.indexed_SE_segments]
        return [(tup + (j,)) for tup, j in zip(parent_start_loops, parent_end_loops)]

    def _child_loops(self, parent_loops):
        child_loops = []
        for parent_start_index, counter, child_id, parent_stop_index in parent_loops:
            if child_id == '1':
                for i, segment in self.indexed_HL_segments:
                    if segment.element(2) == counter:
                        # index of child, parent/tx counter, and if sub-child
                        child_loops.append(
                            (i, counter, segment.element(-1), parent_stop_index))

        # If child_id is greater than 1, recursively call the fn
            if int(child_id) > 1:
                child_loops.extend(self._child_loops(
                    [(parent_start_index, counter, str(int(child_id) - 1), parent_stop_index)]))
        return child_loops


class HierarchicalLoopManager:
    def __init__(self, data, delim_cls=AnsiX12Delim):
        self.hl = HierarchicalLoop(data, delim_cls)
        self.summary = {}
        self.generate_summary()

    def generate_summary(self):
        for pl in self.hl.parent_loops:
            parent_summary = {
                'parent_index_start': pl[0],
                'parent_index_end': pl[-1],
                'children': []
            }
            # Find children loops for this parent tx
            children = []
            for cl in self.hl.child_loops:
                if pl[0] < cl[0] < pl[-1]:
                    children.append({
                        'child_index_start': cl[0],
                        'child_index_stop': cl[-1]
                    })

            # Add children to parent summary
            parent_summary['children'] = children

            # Add HL 1 counter to the summary as the key
            self.summary[pl[1]] = parent_summary


"""
loop_manager = HierarchicalLoopManager(sample_data_837i_edited)  
summary = loop_manager.summary 

output:
{'1': {'parent_index_start': 7,
  'parent_index_end': 35,
  'children': [{'child_index_start': 16, 'child_index_stop': 35}]},
 '63': {'parent_index_start': 41,
  'parent_index_end': 69,
  'children': [{'child_index_start': 50, 'child_index_stop': 69}]},
 '49': {'parent_index_start': 75,
  'parent_index_end': 103,
  'children': [{'child_index_start': 84, 'child_index_stop': 103}]},
 '75': {'parent_index_start': 109,
  'parent_index_end': 138,
  'children': [{'child_index_start': 118, 'child_index_stop': 138}]},
 '79': {'parent_index_start': 144,
  'parent_index_end': 179,
  'children': [{'child_index_start': 153, 'child_index_stop': 179},
   {'child_index_start': 160, 'child_index_stop': 179}]}}
"""
