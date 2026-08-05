import unittest

from ankaflow import (
    Flow,
    FlowContext,
    Stages,
    ConnectionConfiguration,
)

yml = """
- name: TestFoo
  kind: transform
  query: select 'foo' as answer
- name: TestBar
  kind: transform
  query: select 'bar' as answer
"""


class TestDataframe(unittest.TestCase):
    def setUp(self):
        stages = Stages.from_yaml(yml)
        self.f = Flow(stages, FlowContext(), ConnectionConfiguration())
        self.f.run()
    
    def test_pull_foo(self):
        df = self.f.df("TestFoo")
        assert df.iloc[0]["answer"] == "foo"
    
    def test_pull_bar(self):
        df = self.f.df("TestBar")
        assert df.iloc[0]["answer"] == "bar"

