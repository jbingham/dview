# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test for dview"""
from __future__ import absolute_import

from dsub.commands import dview
import logging
import StringIO
import sys
import tempfile
import unittest
import yaml

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def create_pipeline():
  options = PipelineOptions(['--runner=direct', '--staging_location=foo', '--temp_location=foo'])
  return beam.Pipeline(options=options)


class DviewTest(unittest.TestCase):
  """Unit test for dview with direct runner."""

  PROVIDER = '<providers.foobar>'
  SAMPLE_LIST = '- job1\n- job2'
  SAMPLE_LIST_OUTPUT = 'PCollection[job2/Wait.None]'
  SAMPLE_BRANCH = '- job1\n- BRANCH:\n  - job2a\n  - job2b'
  SAMPLE_BRANCH_OUTPUT = 'PCollection[MergeBranches/Combine.globally/InjectDefault.None]'
  MAIN_STDOUT = 'PCollection[job2/Wait.None]\n'

  def test_branched_graph(self):
    p = create_pipeline()
    input = p | beam.Create(['test'])

    args = type('',(object,),{"foo": 1})()

    yaml_string = self.SAMPLE_BRANCH.decode('string_escape')
    graph = yaml.load(yaml_string)

    output = dview.create_graph(graph, input, args)
    self.assertEqual(str(output), self.SAMPLE_BRANCH_OUTPUT)

  def test_linear_graph(self):
    p = create_pipeline()
    input = p | beam.Create(['test'])

    args = type('',(object,),{"foo": 1})()

    yaml_string = self.SAMPLE_LIST.decode('string_escape')
    graph = yaml.load(yaml_string)

    output = dview.create_graph(graph, input, args)
    self.assertEqual(str(output), self.SAMPLE_LIST_OUTPUT)

  def test_dry_run(self):
    stdout = sys.stdout
    sys.stdout = StringIO.StringIO()
    dview.main([
        '--dag',
        '%s' % self.SAMPLE_LIST,
        '--dry-run'])
    self.assertEqual(sys.stdout.getvalue(), self.MAIN_STDOUT)
    sys.stdout = stdout


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
