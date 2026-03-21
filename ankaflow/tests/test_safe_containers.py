import unittest
import pandas as pd
import json

from ..common.renderer import Renderer
from ..common.types import Variables, FlowContext, BaseSafeDict


class TestRenderer(unittest.TestCase):
    def setUp(self):
        self.ctx = FlowContext({"foo": {"bar": 3}, "num": 5})
        self.vars = Variables(**{
            "foo": {"bar": 3},
            "num": 5,
            "num_str": "5",
            "__ver": "12",
            "_ver": "1.2.0",
            "df": pd.DataFrame({"col": [1, 2]})
        })
        self.renderer = Renderer(ctx=self.ctx, vars=self.vars)

    def test_render_5(self):
        # Test basic string rendering
        result = self.renderer.render_string("<< vars.get('num') >>")
        self.assertEqual(result, 5)

    def test_render_bar(self):
        # Test basic string rendering
        result = self.renderer.render_string("<< ctx.foo.bar >>")
        self.assertEqual(result, 3)

    def test_num_str(self):
        # Test basic string rendering
        result = self.renderer.render_string("<< vars.num_str >>")
        self.assertEqual(result, 5)

    def test_dunder_is_empty_string(self):
        result = self.renderer.render_string("<< vars.__ver >>")
        self.assertEqual(result, "")
        self.assertIsInstance(result, str)

    def test_sunder_is_string(self):
        result = self.renderer.render_string("<< vars._ver >>")
        self.assertEqual(result, "1.2.0")
        self.assertIsInstance(result, str)

    def test_context_immutable(self):
        print(self.ctx)
        with self.assertRaises(TypeError):
            self.ctx["num"] = 6

        self.assertEqual(self.ctx["num"], 5)

    def test_df_via_json(self):
        """Verify JSON round trip."""
        df = pd.DataFrame({"col": [1, 2, pd.NA]})
        payload = df.to_json(orient="records")
        self.vars["serialized"] = json.loads(payload)
        self.assertEqual(self.vars.serialized[2].col, None)

    def test_json_to_safedict(self):
        df = pd.DataFrame({"col": [1, 2, pd.NA]})
        payload = df.to_json(orient="records")
        self.vars["serialized"] = json.loads(payload)
        self.assertIsInstance(self.vars.serialized[1], BaseSafeDict)

