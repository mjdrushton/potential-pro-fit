

from atsim.pro_fit.runners._util import BatchNameIterator


def testBatchNameIterator():
  bnm = BatchNameIterator()

  assert bnm.next() == "Batch-1"
  assert bnm.next() == "Batch-2"
  assert bnm.next() == "Batch-3"

def testBatchNameIterator_prefix():
  bnm = BatchNameIterator("Hello")
  assert bnm.next() == "Hello1"
  assert bnm.next() == "Hello2"
  assert bnm.next() == "Hello3"

