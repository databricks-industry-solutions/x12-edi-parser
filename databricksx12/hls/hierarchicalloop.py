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
        self.summary = {}
        self.generate_summary()

    def generate_summary(self):
        def process_parent_loop(parent_loop):
            # filter/map child loops within a parent loop
            children = list(map(
                lambda child_loop: {
                    'child_index_start': child_loop[0],
                    'child_index_stop': child_loop[-1]
                },
                filter(
                    lambda child_loop: parent_loop[0] < child_loop[0] < parent_loop[-1], self.hl.child_loops)
            ))
            # summary dict for each parent
            parent_summary = {
                'parent_index_start': parent_loop[0],
                'parent_index_end': parent_loop[-1],
                'children': children
            }
            return (parent_loop[1], parent_summary)

        # summarize all parent loops
        self.summary = dict(map(process_parent_loop, self.hl.parent_loops))


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
