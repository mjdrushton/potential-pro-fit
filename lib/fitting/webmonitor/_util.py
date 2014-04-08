def calculatePercentageDifference(rowdict):
  try:
    expect =  rowdict['expected_value']
    extract = rowdict['extracted_value']
    return ((extract-expect)/float(expect))*100.0
  except (ZeroDivisionError, ValueError,TypeError):
    return None
