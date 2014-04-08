"""Tests for atomsscripts.fitting.reporters"""

import unittest

from atomsscripts import fitting
from atomsscripts import testutil

import sqlalchemy as sa

class MockJob(object):

  def __init__(self, name, variables, evaluatorRecords):
    self.name = name
    self.variables = variables
    self.evaluatorRecords = evaluatorRecords


class SQLiteReporter(unittest.TestCase):
  """Tests for atomsscripts.fitting.reporters.SQLiteReporter"""

  def setUp(self):
    # Create some MinimizerResults to feed to 
    variables = fitting.fittool.Variables([
      ('A', 1000.0, False),
      ('rho', 0.1, True),
      ('C', 32.0, False) ], [None, (10.0, None), (0.0, 5.0)])
    self.initialVariables = variables

    self.calculatedVariables = fitting.fittool.CalculatedVariables([("E", "A - C"), ("sum", "A+rho+C")])

    mval = 100.0

    meritvals = []
    vinstances = []
    for i in xrange(10):
      mval *= 0.1
      meritvals.append(mval)
      if not vinstances:
        rho = variables.fitValues[0]
      else:
        rho = vinstances[-1].fitValues[0]
      rho *= 0.1
      vinstances.append(variables.createUpdated([rho]))  

    # Create Jobs
    subevals =  [ ["Cell"], ["Penalty", "Bulk"], ["Value"]]

    ER = fitting.evaluators.EvaluatorRecord

    mval = meritvals[0]
    jobs = [
      MockJob("Job 1", vinstances[0], [
        [ER('Cell', 10.0, 9.0, 1.0, 2.0, evaluatorName = "Cell Evaluator")], 
        [ER('Penalty', 0.0, 1.0, 100.0, 100.0, evaluatorName = "Penalty Evaluator"),
         ER('Bulk', 90.0, 101.0, 1.0, 4.0, evaluatorName= "Penalty Evaluator")], 
        [ER('Value', 0.5, 0.8, 1.2, 5.0, evaluatorName = "Value Evaluator")]]),
      MockJob("Job 2", vinstances[0], [
        [ER('Cell2', 10.0, 9.0, 1.0, 2.0, evaluatorName = "Cell Evaluator")], 
        [ER('Penalty2', 0.0, 1.0, 100.0, 100.0, evaluatorName = "Penalty Evaluator"),
         ER('Bulk2', 90.0, 101.0, 1.0, 4.0, evaluatorName= "Penalty Evaluator")], 
        [ER('Value2', 0.5, 0.8, 1.2, 5.0, evaluatorName = "Value Evaluator")]]),
      MockJob("Job 3", vinstances[0], [
        [ER('Cell3', 10.0, 9.0, 1.0, 2.0, evaluatorName = "Cell Evaluator")], 
        [ER('Penalty3', 0.0, 1.0, 100.0, 100.0, evaluatorName = "Penalty Evaluator"),
         ER('Bulk3', 90.0, 101.0, 1.0, 4.0, evaluatorName= "Penalty Evaluator")], 
        [ER('Value3', 0.5, 0.8, 1.2, 5.0, evaluatorName = "Value Evaluator")]]) ]
    
    self.minimizerResults = [fitting.minimizers.MinimizerResults([mval], [(vinstances[0], jobs)])]


  def testInsertSingleMinimizerResult(self):
    """Test insertion of MinimizerResults into database"""
    reporter = fitting.reporters.SQLiteReporter(None, self.initialVariables, self.calculatedVariables)
    #reporter = SQLiteReporter('/Users/mr498/Desktop/db.sqlite', self.initialVariables)
    engine = reporter._saengine
    engine.echo = True
    reporter(self.minimizerResults[0])
    
    with engine.connect() as conn:
      metadata = sa.MetaData(conn)
      metadata.reflect()

      # Check that 'variable_keys' is correctly populated
      table = metadata.tables['variable_keys']
      query = table.select()
      results = conn.execute(query)
      actual = []
      for row in results:
        actual.append(dict(zip(row.keys(), row)))
      results.close()

      expect = [ 
        {'id' : 1, 'variable_name' : 'A', 'fit_flag' : False, 'low_bound' : None, "upper_bound" : None, "calculated_flag" : False, "calculation_expression" : None},
        {'id' : 2, 'variable_name' : 'rho', 'fit_flag' : True, 'low_bound' : float(10.0), "upper_bound" : None, "calculated_flag" : False, "calculation_expression" : None},
        {'id' : 3, 'variable_name' : 'C', 'fit_flag' : False, 'low_bound' : float(0.0), "upper_bound" : float(5.0), "calculated_flag" : False, "calculation_expression" : None},
        {'id' : 4, 'variable_name' : 'E', 'fit_flag' : False, 'low_bound' : None, "upper_bound" : None, "calculated_flag" : True, "calculation_expression" : "A - C"},
        {'id' : 5, 'variable_name' : 'sum', 'fit_flag' : False, 'low_bound' : None, "upper_bound" : None, "calculated_flag" : True, "calculation_expression" : "A+rho+C"}]
      self.assertEquals(expect, actual)

      # Check that 'candidates' table contains what it should
      table = metadata.tables['candidates']
      query = sa.sql.select([metadata.tables['candidates']])

      resultdicts = []    
      results = conn.execute(query)
      for row in results:
        resultdict = dict( zip(row.keys(), row))
        resultdicts.append(resultdict)

      expect = [ {'id' : 1, 
        'iteration_number' : 0,
        'candidate_number' : 0,
        'merit_value' : 10.0}]
      testutil.compareCollection(self, expect, resultdicts)
      results.close()

      # Check that 'variables' table has been correctly populated
      table = metadata.tables['variables']
      query = table.select()
      results = conn.execute(query)
      actual = []
      for row in results:
        actual.append(
          dict(zip(row.keys(), row)) )
      expect = [ {'id' : 1, 'variable_name' : 'A', 'candidate_id' : 1, 'value' : 1000.0},
                 {'id' : 2, 'variable_name' : 'rho', 'candidate_id' : 1, 'value' : 0.01},
                 {'id' : 3, 'variable_name' : 'C', 'candidate_id' : 1, 'value' : 32.0}]
      testutil.compareCollection(self, expect, actual)
      results.close()


      # Check 'jobs' table
      table = metadata.tables['jobs']
      query = table.select()
      results = conn.execute(query)
      actual = []
      for row in results:
        actual.append(
          dict(
            id = row[table.c.id],
            candidate_id = row[table.c.candidate_id],
            job_name = row[table.c.job_name] ))
      expect = [ 
        {'id' : 1, 'candidate_id': 1, 'job_name' : "Job 1"},
        {'id' : 2, 'candidate_id': 1, 'job_name' : "Job 2"},
        {'id' : 3, 'candidate_id': 1, 'job_name' : "Job 3"} ]
      testutil.compareCollection(self, expect, actual)
      results.close()

      # Check 'evaluated' table
      table = metadata.tables['evaluated']
      query = table.select()
      results = conn.execute(query)
      actual = []
      for row in results:
        actual.append(
          dict(
            id = row[table.c.id],
            job_id = row[table.c.job_id],
            evaluator_name = row[table.c.evaluator_name],
            value_name = row[table.c.value_name],
            expected_value = row[table.c.expected_value],
            extracted_value = row[table.c.extracted_value],
            weight = row[table.c.weight],
            merit_value = row[table.c.merit_value],
            evaluatorerror_id = row[table.c.evaluatorerror_id]))

      expect = [
       #Job 1: E1
       dict(
          id = 1, job_id = 1,
          evaluator_name = 'Cell Evaluator',
          value_name = 'Cell', expected_value = 10.0, extracted_value = 9.0,
          weight = 1.0, merit_value = 2.0 ),

      #Job 1: E2
      dict(
          id = 2, job_id = 1,
          evaluator_name = 'Penalty Evaluator',
          value_name = 'Penalty', expected_value = 0.0, extracted_value = 1.0,
          weight = 100.0, merit_value = 100.0),
      dict(
          id = 3, job_id = 1,
          evaluator_name = 'Penalty Evaluator',
          value_name = 'Bulk', expected_value = 90.0, extracted_value = 101.0,
          weight = 1.0, merit_value = 4.0),
      #Job 1: E3   
      dict(
          id = 4, job_id = 1,
          evaluator_name = 'Value Evaluator',
          value_name = 'Value', expected_value = 0.5, extracted_value = 0.8,
          weight = 1.2, merit_value = 5.0),
      #========
      #Job 2: E1
      dict(
        id = 5, job_id = 2,
        evaluator_name = 'Cell Evaluator',
        value_name = 'Cell2', expected_value = 10.0, extracted_value = 9.0,
        weight = 1.0, merit_value = 2.0),

      #Job 2: E2
      dict(
          id = 6, job_id = 2,
          evaluator_name = 'Penalty Evaluator',
          value_name = 'Penalty2', expected_value = 0.0, extracted_value = 1.0,
          weight = 100.0, merit_value = 100.0),
      dict(
          id = 7, job_id = 2,
          evaluator_name = 'Penalty Evaluator',
          value_name = 'Bulk2', expected_value = 90.0, extracted_value = 101.0,
          weight = 1.0, merit_value = 4.0),
      #Job 2: E3   
      dict(
          id = 8, job_id = 2,
          evaluator_name = 'Value Evaluator',
          value_name = 'Value2', expected_value = 0.5, extracted_value = 0.8,
          weight = 1.2, merit_value = 5.0) ,
      #========
       #Job 3: E1
      dict(
        id = 9, job_id = 3,
        evaluator_name = 'Cell Evaluator',
        value_name = 'Cell3', expected_value = 10.0, extracted_value = 9.0,
        weight = 1.0, merit_value = 2.0),

      #Job 3: E2
      dict(
          id =10, job_id = 3,
          evaluator_name = 'Penalty Evaluator',
          value_name = 'Penalty3', expected_value = 0.0, extracted_value = 1.0,
          weight = 100.0, merit_value = 100.0),
      dict(
          id = 11, job_id = 3,
          evaluator_name = 'Penalty Evaluator',
          value_name = 'Bulk3', expected_value = 90.0, extracted_value = 101.0,
          weight = 1.0, merit_value = 4.0),
      #Job 3: E3   
      dict(
          id = 12, job_id = 3,
          evaluator_name = 'Value Evaluator',
          value_name = 'Value3', expected_value = 0.5, extracted_value = 0.8,
          weight = 1.2, merit_value = 5.0)
      ]

      for e in expect:
        e['evaluatorerror_id'] = None

      testutil.compareCollection(self, expect, actual)
      results.close()

  def testStatus(self):
    """Test population of the 'status' table"""
    reporter = fitting.reporters.SQLiteReporter(None, self.initialVariables, self.calculatedVariables,'fitting_run')
    #reporter = SQLiteReporter('/Users/mr498/Desktop/db.sqlite', self.initialVariables)
    engine = reporter._saengine
    engine.echo = True
    
    with engine.connect() as conn:
      # import pudb;pudb.set_trace()
      metadata = sa.MetaData(conn)
      metadata.reflect()

      table = metadata.tables['runstatus']
      query = table.select()
      results = conn.execute(query)

      actual = []
      for row in results:
        actual.append( dict(zip(row.keys(), row)))
      expect = [{'id' : 1, 'runstatus' : 'Running', 'title' : 'fitting_run'}]
      testutil.compareCollection(self, expect, actual)
      results.close()

      # Test finished() method
      reporter.finished()
      actual = []
      results = conn.execute(query)
      for row in results:
        actual.append( dict(zip(row.keys(), row)))
      expect = [{'id' : 1, 'runstatus' : 'Finished', 'title' : 'fitting_run'}]
      testutil.compareCollection(self, expect, actual)
      results.close()

  def testErrorEvaluatorRecord(self):
    """Test for insertion of evaluators.ErrorEvaluatorRecord"""
    reporter = fitting.reporters.SQLiteReporter(None, self.initialVariables,self.calculatedVariables)
    engine = reporter._saengine
    engine.echo = True
    
    with engine.connect() as conn:
      metadata = sa.MetaData(conn)
      metadata.reflect()

      class MockJob(object):
        def __init__(self, erecords):
          self.evaluatorRecords = erecords

      try:
        {}['badkey']
      except Exception as exc:
        mybad = exc

      erecord = fitting.evaluators.ErrorEvaluatorRecord(
        "BadEvalValue",
        10.0,
        mybad,
        12.0,
        "BadEvaluator")

      candidatestable = metadata.tables['candidates']
      conn.execute(candidatestable.insert())


      jobtable = metadata.tables['jobs']
      conn.execute(jobtable.insert(), candidate_id = 1)

      job = MockJob([[erecord]])
      reporter._createEvaluators(conn, 1, job)

      # Check evaluated table
      etable = metadata.tables['evaluated']
      query = etable.select()
      actual = []
      for row in conn.execute(query):
        actual.append( dict(zip(row.keys(), row)))
      expect = [{
        'id' : 1,
        'job_id' : 1,
        'evaluator_name' : 'BadEvaluator',
        'value_name' : 'BadEvalValue',
        'expected_value' : 10.0,
        'extracted_value' : None,
        'weight' : 12.0,
        'merit_value' : None,
        'evaluatorerror_id' : 1
      }]
      testutil.compareCollection(self, expect, actual)

      # Now check the evaluatorerror table
      expect = [{
        'id' : 1,
        'msg' : str(exc)
      }]

      etable = metadata.tables['evaluatorerror']
      query = etable.select()
      actual = []
      for row in conn.execute(query):
        actual.append( dict(zip(row.keys(), row)))
      testutil.compareCollection(self, expect, actual)

