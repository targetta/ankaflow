import unittest
import pandas as pd
import typing as t
import logging
from pydantic import BaseModel, RootModel
from enum import Enum


# ------------------------------
# Dummy models & pipeline logic
# ------------------------------


class FlowContext(BaseModel):
    foo: str = "bar"


class Variables(dict):
    pass


class ConnectionConfiguration(BaseModel):
    pass


class DummyRenderer:
    def __init__(self, context: t.Any, API: t.Any, variables: dict) -> None:
        self.context = context
        self.API = API
        self.variables = variables

    def render(self, templ: t.Any) -> t.Any:
        if isinstance(templ, str) and "<<" in templ and ">>" in templ:
            try:
                val = self.variables["loop_control"]["id"]
                return templ.replace(
                    "<< variables['loop_control']['id'] >>", str(val)
                )
            except Exception:
                return templ
        return templ


class DummyAPI:
    pass


class DummyResult:
    def __init__(self, data: t.Optional[pd.DataFrame] = None) -> None:
        self._data = (
            pd.DataFrame([{"id": 1, "value": "dummy"}])
            if data is None
            else data
        )

    async def df(self) -> pd.DataFrame:
        return self._data


class DummyDDB:
    def __init__(self, dummy_data: t.Optional[pd.DataFrame] = None) -> None:
        self.dummy_data = dummy_data

    async def sql(self, query: str) -> DummyResult:
        return DummyResult(self.dummy_data)

    @classmethod
    async def connect(
        cls, conn_opts: t.Any, dummy_data: t.Optional[pd.DataFrame] = None
    ) -> "DummyDDB":
        return cls(dummy_data)


class DummyConnectionModel(BaseModel):
    kind: str
    module: t.Optional[str] = None
    locator: str
    config: t.Optional[t.Any] = None
    fields: t.List[t.Any] = []


class DummyConnection:
    def __init__(self, idb, name, connection, context, variables, logger):
        self.idb = idb
        self.name = name
        self.connection = connection
        self.context = context
        self.variables = variables
        self.logger = logger

    async def tap(self, query: str) -> None:
        await self.idb.sql(query)

    async def sink(self, name: str) -> None:
        self.logger.info(f"Dummy sink: {name}")

    async def sql(self, query: str) -> DummyResult:
        return await self.idb.sql(query)

    async def show_schema(self, print: bool = False) -> t.List[dict]:
        return [
            {"name": "id", "type": "int"},
            {"name": "value", "type": "text"},
        ]


# ------------------------------
# Minimal pipeline models
# ------------------------------


class FieldModel(BaseModel):
    name: str
    type: str


class DatablockDef(BaseModel):
    kind: str
    name: str
    locator: t.Optional[str] = None
    connection: t.Optional[DummyConnectionModel] = None
    skip_if: t.Optional[str] = None
    query: t.Optional[str] = None
    context: t.Optional[FlowContext] = None
    show: int = 0
    show_schema: t.Optional[bool] = False
    stages: t.Optional[t.List[t.Any]] = None
    on_error: str = "fail"
    throttle: t.Optional[t.Union[int, float]] = None
    log_level: t.Optional[Enum] = None
    fields: t.Optional[t.List[FieldModel]] = None


class Stages(RootModel[t.List[DatablockDef]]):
    def steps(self):
        return iter(self.root)


class Datablock:
    def __init__(
        self,
        conn,
        defs,
        context,
        default_connection,
        variables,
        logger,
        prevous_stage=None,
    ):
        self.idb = conn
        self.defs = defs
        self.context = context
        self.default_connection = default_connection
        self.variables = variables
        self.logger = logger
        self.prevous_stage = prevous_stage
        self.renderer = DummyRenderer(context, DummyAPI(), variables)

    def render(self, query: str):
        return self.renderer.render(query)

    async def transform(self):
        await self.idb.sql(self.render(self.defs.query))
        return self.defs.name

    async def sink(self):
        return await DummyConnection(
            self.idb,
            self.defs.name,
            self.defs.connection,
            self.context,
            self.variables,
            self.logger,
        ).sink(self.defs.name)

    async def internal(self):
        await self.idb.sql(self.defs.query)
        return self.defs.name

    async def do(self):
        stages = Stages.parse_obj(self.defs.stages or [])
        for step in stages.steps():
            sub = Datablock(
                conn=self.idb,
                defs=step,
                context=self.context,
                default_connection=self.default_connection,
                variables=self.variables,
                logger=self.logger,
            )
            await sub.transform()


class AsyncFlow:
    def __init__(
        self, defs, context, default_connection, variables, logger, conn
    ):
        self.defs = defs
        self.context = context
        self.default_connection = default_connection
        self.variables = variables
        self.logger = logger
        self.conn = conn
        self.results = []

    async def run(self):
        for defn in self.defs:
            db = Datablock(
                self.conn,
                defn,
                self.context,
                self.default_connection,
                self.variables,
                self.logger,
            )
            await db.transform()
            self.results.append(db)

    async def df(self):
        result = await self.conn.sql("SELECT 1")
        return await result.df()


# ------------------------------
# Actual Test Suite
# ------------------------------


class TestPipeline(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.context = FlowContext(foo="test")
        self.variables = Variables({"loop_control": {"id": 42}})
        self.default_conn = ConnectionConfiguration()
        self.logger = logging.getLogger("unit_test")

    async def test_render_method_with_loop_control(self):
        dummy_def = DatablockDef(
            kind="transform",
            name="test_block",
            query="<< variables['loop_control']['id'] >>",
        )
        db = Datablock(
            conn=await DummyDDB.connect({}),
            defs=dummy_def,
            context=self.context,
            default_connection=self.default_conn,
            variables=self.variables,
            logger=self.logger,
        )
        result = db.render(dummy_def.query)  # type: ignore[assignment]
        self.assertEqual(result, "42")

    async def test_transform_stage_execution(self):
        transform_def = DatablockDef(
            kind="transform",
            name="transform_test",
            query="SELECT 1 AS id",
            show=5,
        )
        db = Datablock(
            conn=await DummyDDB.connect({}),
            defs=transform_def,
            context=self.context,
            default_connection=self.default_conn,
            variables=Variables(),
            logger=self.logger,
        )
        result = await db.transform()
        self.assertEqual(result, "transform_test")

    async def test_sink_stage_execution(self):
        sink_def = DatablockDef(
            kind="sink",
            name="sink_test",
            connection=DummyConnectionModel(
                kind="Variable", locator="dummy_sink"
            ),
        )
        db = Datablock(
            conn=await DummyDDB.connect({}),
            defs=sink_def,
            context=self.context,
            default_connection=self.default_conn,
            variables=Variables(),
            logger=self.logger,
        )
        result = await db.sink()
        self.assertIsNone(result)

    async def test_internal_stage_execution(self):
        internal_def = DatablockDef(
            kind="internal", name="internal_test", query="SELECT 1 AS id"
        )
        db = Datablock(
            conn=await DummyDDB.connect({}),
            defs=internal_def,
            context=self.context,
            default_connection=self.default_conn,
            variables=Variables(),
            logger=self.logger,
        )
        result = await db.internal()
        self.assertEqual(result, "internal_test")

    async def test_pipeline_stage_loop(self):
        pipeline_def = DatablockDef(
            kind="pipeline",
            name="pipeline_test",
            stages=[
                {
                    "kind": "transform",
                    "name": "loop_transform",
                    "query": (
                        "SELECT generate_series(1, 2) AS loop_val, "
                        "<< variables['loop_control']['id'] >> AS parent_id"
                    ),
                    "show": 2,
                }
            ],
        )
        db = Datablock(
            conn=await DummyDDB.connect({}),
            defs=pipeline_def,
            context=self.context,
            default_connection=self.default_conn,
            variables=Variables({"loop_control": {"id": 42}}),
            logger=self.logger,
            prevous_stage="dummy_prev",
        )
        result = await db.do()
        self.assertIsNone(result)

    async def test_async_flow_run_and_df(self):
        transform_def = DatablockDef(
            kind="transform",
            name="flow_transform",
            query="SELECT 1 AS id, 'value' AS val",
            show=5,
        )
        sink_def = DatablockDef(
            kind="sink",
            name="flow_sink",
            connection=DummyConnectionModel(
                kind="Variable", locator="dummy_sink"
            ),
        )
        expected_df = pd.DataFrame([{"id": 1, "val": "value"}])
        flow = AsyncFlow(
            defs=[transform_def, sink_def],
            context=self.context,
            default_connection=self.default_conn,
            variables=Variables(),
            logger=self.logger,
            conn=await DummyDDB.connect({}, dummy_data=expected_df),
        )
        await flow.run()
        df = await flow.df()
        self.assertFalse(df.empty)
        self.assertIn("id", df.columns)
        self.assertIn("val", df.columns)


if __name__ == "__main__":
    unittest.main()
