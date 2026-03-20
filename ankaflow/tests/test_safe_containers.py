import unittest

from ..common.renderer import Renderer
from ..models import Variables, FlowContext


class TestRenderer(unittest.TestCase):
    def setUp(self):
        ctx = FlowContext({"foo": {"bar": 3}, "num": 5})
        vars = Variables(**{
            "foo": {"bar": 3},
            "num": 5,
            "num_str": "5",
            "__ver": "12",
            "_ver": "1.2.0",
        })
        self.renderer = Renderer(ctx=ctx, vars=vars)

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

    def test_sunder_is_stirng(self):
        result = self.renderer.render_string("<< vars._ver >>")
        self.assertEqual(result, "1.2.0")
        self.assertIsInstance(result, str)
