from atsim.pro_fit.runners._util import BatchNameIterator


def testBatchNameIterator():
    bnm = BatchNameIterator()

    assert next(bnm) == "Batch-1"
    assert next(bnm) == "Batch-2"
    assert next(bnm) == "Batch-3"


def testBatchNameIterator_prefix():
    bnm = BatchNameIterator("Hello")
    assert next(bnm) == "Hello1"
    assert next(bnm) == "Hello2"
    assert next(bnm) == "Hello3"
