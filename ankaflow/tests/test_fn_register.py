# ruff: noqa
import unittest
import duckdb
import typing as t

from ..internal.macros import Fn, register_macro, iter_macros, _registered_macros

class TestRegisterMacros(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.con = duckdb.connect(database=":memory:")

    # def tearDown(self) -> None:
    #     self.con.close()

    def init_all(self):
        self.con.execute("CREATE SCHEMA Fn;")
        for name, body in iter_macros().items():
            macro = f"""CREATE OR REPLACE MACRO Fn.{name}{body};"""
            self.con.execute(macro)

    def test_register_macro(self):
        register_macro("answer", "() AS 42;")
        self.assertIn("answer", _registered_macros)


    def test_strip_trail(self):
        register_macro("answer", "() AS 42; ")
        self.assertEqual(_registered_macros["answer"], "() AS 42")

    def test_register_existing(self):
        with self.assertRaises(ValueError):
            register_macro("dt", "() AS 42;")

    def test_retrieve_registered(self):
        register_macro("answer", "() AS 42;")

        macros = iter_macros()

        self.assertIn("answer", macros)

    def test_register_and_exec(self):
        register_macro("answer", "() AS 42;")
        self.init_all()
        res = self.con.execute("SELECT Fn.answer()").fetchone()
        self.assertIn(42, t.cast(t.Tuple[t.Any],res))