import unittest
import os

from ..common.renderer import Renderer
from ..common.security import install_environment_protection

install_environment_protection()


class TestRendererSecurity(unittest.TestCase):
    def test_leaked_object_dunder_block(self):
        """
        Scenario: Accidentally passed the 'os' module into the renderer.
        Goal: Ensure the DunderGate prevents reaching .__class__ or .__globals__
        """
        # Pass 'os' into kwargs
        renderer = Renderer(leaked_os=os)

        # 1. Attempt to reach dunders on the leaked object
        # This hits BaseSafeDict.__getitem__ for 'leaked_os',
        # then tries to hit __class__ which is BLOCKED by the gate.
        result = renderer.render_string("<< leaked_os.__class__ >>")

        # Result should be empty/Undefined because the Dunder-Gate
        # raises KeyError/AttributeError which Jinja ignores.
        self.assertEqual(result, "")

    def test_leaked_object_env_masking(self):
        """
        Scenario: Passes 'os' into kwargs.
        Goal: Ensure even if they call a valid method like getenv(),
        it returns None.
        """
        os.environ["SYSTEM_SECRET"] = "TOP_SECRET_PROXIMA"
        renderer = Renderer(leaked_os=os)

        # 2. Attempt to call a legitimate method on the leaked object
        # Since it's inside the render_string's secure_context(),
        # the global monkeypatch intercepts the call.
        result = renderer.render_string(
            "<< leaked_os.getenv('SYSTEM_SECRET') >>"
        )

        # Result must be None/Empty string
        self.assertIn(result, [None, ""])

    def test_dunder_gate_direct(self):
        """Verify that __ver (a dunder) is blocked even if passed in."""
        renderer = Renderer(__ver="1.2.0", public_var="hello")

        # Direct dunder access
        result = renderer.render_string("<< __ver >>")
        self.assertIsNone(result)

        # Public access still works
        result = renderer.render_string("<< public_var >>")
        self.assertEqual(result, "hello")

    def test_nested_dict_sanitization(self):
        """Verify that nested dictionaries are automatically wrapped in BaseSafeDict."""  # noqa: E501
        renderer = Renderer(data={"nested": {"__secret": "hidden"}})

        # Accessing the nested dunder should still fail
        result = renderer.render_string("<< data.nested.__secret >>")
        self.assertEqual(result, "")


if __name__ == "__main__":
    unittest.main()
