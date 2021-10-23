import random
from pyarrow import compute as pc, feather
import pyarrow as pa
from functools import total_ordering
from typing import List, Optional, Union
from pathlib import Path
import uuid
from typing import List, Set, Dict, Tuple, Optional, Iterator, Union, Any


@total_ordering
class Tablet():
    """
    A wrapper around pyarrow.Table that includes a min and max vector and caching to disk.
    """
    def __init__(self, tab : Union[pa.Table, "Tablet"], dir, min : List[pa.Scalar], max : List[pa.Scalar], keys):
        self.path = Path(dir) / (str(uuid.uuid1()) + ".feather")
        if isinstance(tab, Tablet):
            self._table : pa.Table = tab._table
            self.min : List[pa.Scalar] = tab.min
            self.max : List[pa.Scalar] = tab.max
        elif isinstance(tab, pa.Table):
            assert min is not None
            if max is None:
                max = tb_max(tab, keys)
            if min is None:
                min = tb_max(tab, keys)
            self.min = min
            self.max = max
            self._table = tab
        else:
            raise TypeError("Can't handle tab of type " + type(tab))
        assert tab is not None

    def __repr__(self):
        mins = ", ".join([str(el.as_py()) for el in self.min])
        maxes = ", ".join([str(el.as_py()) for el in self.max])
        return f"Tablet: ({mins}) <-> ({maxes})"
    def flush(self):
        if self._table is not None:
            feather.write_feather(self._table, self.path)
            self._table = None
    def __getattr__(self, key : str):
        return getattr(self.table, key)
    def __getitem__(self, key):
        return self.table.__getitem__(key)
    def __lt__(self, other : Union["Tablet", List[pa.Scalar]]):
        if isinstance(other, list):
            cmp = other
        else:
            cmp = other.min
        return vec_lt(self.max, cmp)
    def __len__(self) -> int:
        return len(self.table)
    def __gt__(self,  other : "Tablet"):
        if isinstance(other, list):
            cmp = other
        else:
            cmp = other.max
        return vec_gt(self.min, cmp)
    def __eq__(self, other):
        determined = (self > other) or self < other
        return not determined
        self._table = None
    @property 
    def table(self) -> pa.Table:
        if self._table is not None:
            return self._table
        self._table = feather.read_table(self.path, memory_map = True)
        return self._table
    def destroy(self):
        """
        Manual cleanup immediately.
        """
        self._table = None
        if self.path.exists():
            self.path.unlink()

def vec_lt(left : List[pa.Scalar], right: List[pa.Scalar]) -> bool:
    for a, b in zip(left, right):
        if pc.less(a, b).as_py():
            return True
        if pc.greater(a, b).as_py():
            return False
    return False

def flatten(t : List[List[Any]]) -> List[Any]:
    # God I wish this was in the python stdlib
    return [item for sublist in t for item in sublist]

def vec_gt(left : List[pa.Scalar], right : List[pa.Scalar], oreq : bool = False) -> bool:
    for a, b in zip(left, right):
        if pc.greater(a, b).as_py():
            return True
        if pc.less(a, b).as_py():
            return False
    if len(left) > 0 and len(right) > 0 and oreq:
        return True
    return False

def vec_gte(left : List[pa.Scalar], right : List[pa.Scalar]) -> bool:
    return vec_gt(left, right, oreq = True)

def min_val(*tbs : pa.Table) -> List[pa.Scalar]:
    mini : List[pa.Scalar] = []
    for tb in tbs:
        if len(mini) == 0:
            mini = tb.min
        elif vec_lt(tb.min, mini):
            mini = tb.min
    return mini

def total_batch_nrows(arrlist : List[Tablet]) -> int:
    return sum([len(a) for a in arrlist])

def total_batch_size(arrlist : List[Tablet]) -> int:
    return sum([a.nbytes for a in arrlist])

def max_val(*tbs : pa.Table) -> List[pa.Scalar]:
    maxi : List[pa.Scalar] = []
    for tb in tbs:
        if len(maxi) == 0:
            maxi = tb.max
        elif vec_gt(tb.max, maxi):
            maxi = tb.max
    if len(maxi) == 0:
        print(*tbs)
        raise TypeError("No maximum found")
    return maxi


def tb_min(tb : Union[pa.Table, "Tablet"], keys, func = "min") -> List[pa.Scalar]:
    if len(tb) == 0:
      return []
    if isinstance(tb, Tablet):
        tb = tb.table
    key, *keys = keys
    try:
      minimum = pc.min_max(tb[key])[func]
    except pa.ArrowNotImplementedError:
      # For string values.
      if func == "min":
        n = 0
      elif func == "max":
        n = len(tb) - 1
      else:
        raise NotImplementedError("Must be min or max")
      if n >= len(tb):
        assert n < len(tb)
      minimum_row = pc.partition_nth_indices(tb[key].combine_chunks(), pivot = n)[n].as_py()
      minimum = tb[key][minimum_row]
    filtered = pc.filter(tb, pc.equal(tb[key], minimum))
    if len(filtered) == 1 or len(keys) == 0:
        return [minimum, *[filtered[key][0] for key in keys]]
    return [minimum, *tb_min(filtered, keys, func = func)]

def tb_max(tb, keys):
    return tb_min(tb, keys, func = "max")



def get_pivot(array, keys):
    current = random.choices(array, weights = map(len, array), k = 1)[0]
    if len(current) == 1:
        return [current[k][0] for k in keys]
    elif len(current) == 0:
        return get_pivot(array, keys)
    else:
        rix = random.randint(0, len(current)-1)    
        return [current[k][rix] for k in keys]

        pivots = []
        for i in range(3):
            rix = random.randint(0, len(current)-1)    
            pivots.append([current[k][rix] for k in keys])
        A, B, C = pivots
        if vec_lt(A, B):
            minAB = A
            maxAB = B
        else:
            minAB = B
            maxAB = A
        if vec_gt(C, maxAB):
            return maxAB
        elif vec_lt(C, minAB):
            return minAB
        else:
            return C

def partition(array : List[Tablet], pivot : List[pa.Scalar]) -> Tuple[List[Tablet], List[Tablet], List[Tablet]]:
    before = []
    overlaps = []
    after = []
    for arr in array:
        if vec_lt(arr.max, pivot):
            before.append(arr)
        elif vec_gte(arr.min, pivot):
            after.append(arr)
        else:
            overlaps.append(arr)
    return before, overlaps, after



def consume_tablets(*tablets : Tablet, keys = None):
    assert keys is not None
    for tablet in tablets:
        assert isinstance(tablet, Tablet)
    dir = tablets[0].path.parent
    tb = pa.concat_tables([t.table for t in tablets])
    min = min_val(*tablets)
    max = max_val(*tablets)
    for tablet in tablets:
        tablet.destroy()
    return Tablet(tb, dir, min = min, max = max, keys = keys)
