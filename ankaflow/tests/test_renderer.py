import unittest
import textwrap

from ..common.renderer import Renderer

class TestRenderer(unittest.TestCase):

    def setUp(self):
        self.renderer = Renderer(name="Test", value=123)

    def test_render_string(self):
        # Test basic string rendering
        result = self.renderer.render_string("Hello <<name>>!")
        self.assertEqual(result, "Hello Test!")

        # Test with no variables
        result = self.renderer.render_string("Hello World!")
        self.assertEqual(result, "Hello World!")

        # Test with non-string input
        result = self.renderer.render_string(123) # type: ignore
        self.assertEqual(result, 123)

    def test_render_dict(self):
        # Test rendering a dictionary
        input_dict = {"key1": "Hello <<name>>!", "key2": {"nested_key": "Value <<value>>"}}  # noqa: E501
        expected_output = {"key1": "Hello Test!", "key2": {"nested_key": "Value 123"}}
        result = self.renderer.render(input_dict)
        self.assertEqual(result, expected_output)

    def test_render_list(self):
        # Test rendering a list
        input_list = ["Hello <<name>>!", ["Nested <<value>>"]]
        expected_output = ["Hello Test!", ["Nested 123"]]
        result = self.renderer.render(input_list)
        self.assertEqual(result, expected_output)

    def test_render_json_string(self):
        # Test rendering a JSON string
        input_str = '@json{"key": "Hello <<name>>!"}'
        expected_output = {"key": "Hello Test!"}
        result = self.renderer.render(input_str)
        self.assertEqual(result, expected_output)

    def test_render_old_json_string(self):
        # Test rendering a JSON string
        input_str = 'JSON>{"key": "Hello <<name>>!"}'
        expected_output = {"key": "Hello Test!"}
        result = self.renderer.render(input_str)
        self.assertEqual(result, expected_output)

    def test_render_invalid_json(self):
        # Test invalid JSON string
        input_str = '@json{"key": "Hello <<name>>!'
        with self.assertRaises(ValueError):
            self.renderer.render(input_str)

    def test_render_non_string_input(self):
        # Test non-string input (integer)
        input_int = 123
        result = self.renderer.render(input_int) # type: ignore
        self.assertEqual(result, input_int)

        # Test non-string input (float)
        input_float = 123.45
        result = self.renderer.render(input_float) # type: ignore
        self.assertEqual(result, input_float)

        # Test non-string input (boolean)
        input_bool = True
        result = self.renderer.render(input_bool) # type: ignore
        self.assertEqual(result, input_bool)

    def test_render_multiline_string(self):
        # Test multiline string rendering
        input_str = "Hello <<name>>!\nThis is a test."
        expected_output = "Hello Test!\nThis is a test."  # newline preserved
        result = self.renderer.render_string(input_str)
        self.assertEqual(result, expected_output)

    def test_render_json_multiline_block(self):
        self.renderer = Renderer(user={"id": 888, "active": True})

        input_json = textwrap.dedent("""\
                @json{
                    "id": <<user.id>>,
                    "active": <<user.active|tojson>>
                }
            """)
        result = self.renderer.render(input_json)

        expected = {
            "id": 888,
            "active": True
        }

        self.assertEqual(result, expected)

if __name__ == "__main__":
    unittest.main()
