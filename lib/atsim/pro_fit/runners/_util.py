import itertools


class BatchNameIterator(object):
    """Used to produce a series of Batch names"""

    def __init__(self, prefix="Batch-"):
        """@param prefix Prefix used to name batches"""
        self.prefix = prefix
        start = 1
        self.count = itertools.count(start)
        self.index = start
        self._iter = iter(self)

    def __iter__(self):
        for c in self.count:
            s = self.prefix + str(c)
            yield s

    def __next__(self):
        return next(self._iter)
