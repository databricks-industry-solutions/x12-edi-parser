from databricksx12.edi import *
import functools


class HierarchicalLoop(EDI):
    def __init__(self, data, delim_cls=AnsiX12Delim):
        super().__init__(data, delim_cls)
        self.parent_start_loops = self._parent_start_tup_loops()  # returns tuple; to check
        self.parent_end_loops = self._parent_end_loops()
        self.parent_loops = self._parent_loops()
        self.child_loops = self._child_loops(self.parent_loops)
        self.subchild_loops = self._child_loops(self.child_loops) # recursive cases

    def _parent_start_tup_loops(self):
        # index of parent, counter, and if child
        # TODO unit test to return tuple
        return [(i, x.element(1), x.element(-1)) for i, x in self.segments_by_name_index("HL") if x.element(2) == ""]

    def _parent_end_loops(self):
        return [i for i, x in self.segments_by_name_index("SE")]

    def _parent_loops(self):
        return [(tup + (j,)) for tup, j in zip(self.parent_start_loops, self.parent_end_loops)]

    def _child_loops(self, parent_loops):
        child_loops = [(i, counter, segment.element(-1), parent_stop_index)
                       for _, counter, child_id, parent_stop_index in parent_loops if int(child_id) == 1
                       for i, segment in self.segments_by_name_index("HL") if segment.element(2) == counter]

        # recursive cases where child_id is greater than 1 == sub_child
        subchild_cases = filter(lambda x: int(x[2]) > 1, parent_loops)
        subchild_loops = map(
            lambda x: self._child_loops(
                [(x[0], x[1], str(int(x[2]) - 1), x[3])]),
            subchild_cases
        )
        return functools.reduce(lambda acc, lst: acc + lst, subchild_loops, child_loops)


class HierarchicalLoopManager:
    def __init__(self, data, delim_cls=AnsiX12Delim):
        self.hl = HierarchicalLoop(data, delim_cls)
        self.summary = self.generate_summary()

    def get_child_loops(self, parent_loop, loops):
        return [loop for loop in loops if parent_loop[0] < loop[0] < parent_loop[-1]]

    def process_loop(self, loop, level=0):
        child_loops = self.get_child_loops(loop, self.hl.child_loops)
        children = [self.process_loop(child, level + 1) for child in child_loops]
        
        loop_summary = {
            'index_start': loop[0],
            'index_end': loop[-1],
            'children': children or None
        }
        return loop_summary

    def generate_summary(self):
        """Generate a hierarchical summary for each top-level parent loop."""
        loop_processing = lambda loop: (str(loop[1]), self.process_loop(loop))
        return dict(map(loop_processing, self.hl.parent_loops))


"""
loop_manager = HierarchicalLoopManager(sample_data_837i_edited)  
summary = loop_manager.summary 

output:
{'1': {'index_start': 7,
  'index_end': 35,
  'children': [{'index_start': 16, 'index_end': 35, 'children': None}]},
 '63': {'index_start': 41,
  'index_end': 69,
  'children': [{'index_start': 50, 'index_end': 69, 'children': None}]},
 '49': {'index_start': 75,
  'index_end': 103,
  'children': [{'index_start': 84, 'index_end': 103, 'children': None}]},
 '75': {'index_start': 109,
  'index_end': 138,
  'children': [{'index_start': 118, 'index_end': 138, 'children': None}]},
 '79': {'index_start': 144,
  'index_end': 179,
  'children': [{'index_start': 153,
    'index_end': 179,
    'children': [{'index_start': 160, 'index_end': 179, 'children': None}]},
   {'index_start': 160, 'index_end': 179, 'children': None}]}} 
   # the last 'children' list, there is a repeat that is tricky to remove
"""
