from databricksx12.edi import *

import itertools

class HierarchicalLoop(EDI):
    def __init__(self, data, delim_cls=AnsiX12Delim):
        super().__init__(data, delim_cls)

        # parent and children loops
        self.parent_start_loops = self._parent_start_tup_loops()
        self.parent_end_loops = self._parent_end_loops()
        self.parent_loops = self._parent_loops()
        self.child_loops = self._child_loops(self.parent_loops)
        self.subchild_loops = self._subchild_loops(self.child_loops)
    
    def _parent_start_tup_loops(self):
        # index of parent, counter, and if child 
        return [(i, x.element(1), x.element(-1)) for i, x in self.segments_by_name_index("HL") if x.element(2) == ""] # TODO unit test to return tuple

    def _parent_end_loops(self):
        return [i for i, x in self.segments_by_name_index("SE")]
    
    def _parent_loops(self):
        return [(tup + (j,)) for tup, j in zip(self.parent_start_loops, self.parent_end_loops)]


    def _child_loops(self, parent_loops):
        child_loops = [(i, counter, segment.element(-1), parent_stop_index)
                    for _, counter, child_id, parent_stop_index in parent_loops
                    for i, segment in self.segments_by_name_index("HL") if segment.element(2) == counter]
        return child_loops

    def _subchild_loops(self, child_loops):
        it1, it2 = itertools.tee(child_loops)
        next(it2, None) 
        return [pair[1] for pair in zip(it1, it2) if int(pair[0][2]) == 1]




class HierarchicalLoopManager:
    def __init__(self, data, delim_cls=AnsiX12Delim):
        self.hl = HierarchicalLoop(data, delim_cls)
        self.summary = self.generate_summary()

    def get_child_loops(self, parent_loop, loops):
        """Filter child loops that fall within the given parent loop's range."""
        return list(filter(lambda x: parent_loop[0] < x[0] < parent_loop[3], loops))

    def calculate_child_end_index(self, current_child, next_child, parent_end):
        """Calculate the end index of a child, adjusting to avoid overlap with the next child."""
        return min(current_child[3], next_child[0] - 1 if next_child else parent_end)
    
    def process_child(self, child, subchildren, parent_end):
        """Map a single child loop to its dictionary representation, including subchildren if applicable."""
        return {
            'index_start': child[0],
            'index_end': self.calculate_child_end_index(child, None, parent_end),  # No next_child directly handled here
            'children': subchildren if subchildren else None
        }
    
    def add_subchildren_to_children(self, children, subchild_loops, parent_end):
        """Map function to add subchildren to corresponding children, exclude children that are subchildren."""
        #subchild_ids = {sc[1] for sc in subchild_loops}  # Set of parent_ids for subchildren
        children = [child for child in children if child not in subchild_loops]  # Filter out subchildren
        # subchild_lookup = {sc[1]: sc for sc in subchild_loops}  # Lookup for subchildren by parent counter
        
        # Process each child, include subchildren when applicable
        return list(map(lambda child: self.process_child(
            child,
            [{ 'index_start': subchild[0],
               'index_end': self.calculate_child_end_index(subchild, None, child[-1]),
               'children': None } 
             for subchild in subchild_loops if subchild[1] == child[1] and int(child[2]) == 1],
            parent_end),
            children))

    def process_loop(self, loop):
        child_loops = sorted(self.get_child_loops(loop, self.hl.child_loops), key=lambda x: x[0])
        children = self.add_subchildren_to_children(child_loops, self.hl.subchild_loops, loop[3])
        return {
            'index_start': loop[0],
            'index_end': loop[3],
            'children': children or None
        }

    def generate_summary(self):
        return {str(loop[1]): self.process_loop(loop) for loop in self.hl.parent_loops}






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
  'index_end': 186,
  'children': [{'index_start': 153, 'index_end': 186, 'children': None},
   {'index_start': 160,
    'index_end': 186,
    'children': [{'index_start': 167, 'index_end': 186, 'children': None}]}]}}
"""
"""
sample_data_837p = open("./sampledata/837/837p.txt", "rb").read().decode("utf-8").replace("\\n", "")
HierarchicalLoopManager(sample_data_837p).summary
{'1': {'index_start': 7,
  'index_end': 42,
  'children': [{'index_start': 12, 'index_end': 42, 'children': None},
   {'index_start': 27, 'index_end': 42, 'children': None}]}}
"""
