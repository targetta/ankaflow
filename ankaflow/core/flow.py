# TODO: fix all inconsistent assignments
# type: ignore[assignment]
from textwrap import dedent as dd
import typing as t
import duckdb
import arrow
from duckdb import CatalogException
import logging
import pandas as pd
from asyncio import sleep
from pydantic import BaseModel

from .policy import FlowControl
from .. import models as m
from .. import errors as e
from ..api import API
from ..internal import DDB
from .. import connections as c
from ..common.util import (
    print_df,
    print_error,
    asyncio_run,
    string_to_bool,
    null_logger,
)
from ..common.renderer import Renderer

log = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Stage Handler Abstractions
# ------------------------------------------------------------------------------


class BaseStageHandler:
    """Abstract base class for stage execution handlers.

    Attributes:
        datablock (Datablock): The datablock instance holding execution context.
    """

    require_connection = False

    def __init__(self, block: "StageBlock") -> None:
        """
        Args:
            block (StageBlock): The stage to be executed.
        """
        self.block = block
        self.defs = block.defs
        self.log = block.log
        self.default_connection = block.default_connection
        self.idb = block.idb
        self.ctx = block.ctx
        self.vars = block.vars
        self._connection: c.Connection | None = None

    async def _show(self, limit: t.Union[int, float]) -> str:
        """Shared helper to display preview of a table/view."""
        try:
            if not limit:
                return "Limit not set"
            if limit > 1:
                rel = await self.idb.sql(
                    f'SELECT * FROM "{self.defs.name}" USING SAMPLE {limit} ROWS'  # noqa:E501
                )  # noqa:E501
            elif 0 < limit < 1:
                pct = round(limit * 100)
                rel = await self.idb.sql(
                    f'SELECT * FROM "{self.defs.name}" USING SAMPLE {pct}%'
                )
            else:
                rel = await self.idb.sql(f'SELECT * FROM "{self.defs.name}"')
            df = await rel.df()
            return f"{self.defs.name}\n{print_df(df)}"
        except CatalogException:
            return f"{self.defs.name}\nTable does not exist. Cannot show."

    async def _init_connection(self) -> None:
        if self._connection is not None:
            return

        conn_model = self.defs.connection

        if self.require_connection and not conn_model:
            raise e.FlowError(
                f"Stage '{self.defs.name}' requires a connection."
            )

        if not conn_model:
            self._connection = None
            return

        if not conn_model.config:
            conn_model.config = self.block.default_connection

        conn_cls = c.load_connection(conn_model)
        self._connection = conn_cls(
            self.idb,
            self.defs.name,
            conn_model,
            self.ctx,
            self.vars,
            logger=self.log,
        )

    async def execute(self) -> t.Union[str, None]:
        """Executes the stage.

        Returns:
            t.Union[str, None]: Result of the stage execution.
        """
        raise NotImplementedError("Must implement execute() in subclass.")

    async def show_schema(self) -> t.Optional[m.core.SchemaItem]:
        return None


class PipelineStageHandler(BaseStageHandler):
    """Handles execution for pipeline stages, including loop implementation."""

    async def execute(self) -> t.Union[str, None]:
        """
        Executes a pipeline stage.

        Returns:
            t.Union[str, None]: Result of the pipeline stage execution.
        """
        db = self.block
        if db.prev:
            # Pipeline looping over previous stage results
            rel = await db.idb.sql(f'SELECT * FROM "{db.prev}"')
            df = await rel.df()
            items = df.to_dict(orient="records")
            for item in items:
                # Create new variables for each loop iteration
                db.vars["loop_control"] = item
                # Create a deep copy of the definitions for re-rendering
                local_copy = db.defs.model_copy(deep=True)  # <- Pydantic 2-safe
                local = db.prepare(local_copy)
                # local: m.Datablock = db.prepare(deepcopy(db.defs))
                if bool(local.skip_if):
                    db.log.info(f"Skip {local.name}")
                    continue
                flow = AsyncFlow(
                    local.stages,
                    db.ctx,
                    db.default_connection,
                    variables=db.vars,
                    logger=db.log,
                    conn=db.idb,
                    log_context=item,
                )
                try:
                    await flow.run()
                except Exception as err:
                    db.log.error(
                        print_error(
                            "Failure in flow",
                            err,
                            "Current control variable:",
                            item,
                        )
                    )
                    raise
                finally:
                    db.vars.pop("loop_control", None)
            return None
        else:
            # Pipeline with no loop
            step = db.prepare(db.defs)
            if bool(step.skip_if):
                db.log.info(f"Skip {step.name}")
                return None
            flow = AsyncFlow(
                step.stages,
                db.ctx,
                db.default_connection,
                variables=db.vars,
                logger=db.log,
                conn=db.idb,
            )
            await flow.run()
            return None

    # TODO: pull schema from flow
    async def show_schema(self) -> t.Optional[m.core.SchemaItem]:
        self.log.warning(
            f"Pipeline stage '{self.block.defs.name}' has no schema"
        )
        return None


class TapStageHandler(BaseStageHandler):
    """Handles execution for source stages."""

    require_connection = True

    async def execute(self) -> t.Union[str, None]:
        """
        Executes a source stage.

        Returns:
            t.Union[str, None]: Name of the created source table.
        """
        src = self.defs
        name = src.name
        conn = self._connection
        await self.idb.inject_secrets(src.name, src.connection.config)
        try:
            await conn.tap(query=src.query)
        except Exception as e:
            log.error(e)
            await self.idb.sql(f'DROP TABLE IF EXISTS "{name}"')
            raise
        if src.show_schema:
            try:
                fields = await conn.show_schema()
                self.log.info(fields.print())
            except Exception as e:
                self.log.warning(f"Cannot show schema: {e}")
        if src.show:
            self.log.info(await self._show(src.show))
        return name

    async def show_schema(self) -> t.Optional[m.core.SchemaItem]:
        await self._init_connection()
        conn = self._connection
        if not conn:
            return None

        try:
            fields = await conn.show_schema()
            self.log.info(fields.print())
        except NotImplementedError:
            return None
        except Exception as e:
            self.log.error(e)
            raise

        return m.core.SchemaItem(
            table=self.defs.name,
            locator=self.defs.connection.locator,
            kind=self.defs.connection.kind,
            fields=fields,
        )


class TransformStageHandler(BaseStageHandler):
    """Handles execution for transform stages."""

    async def execute(self) -> t.Union[str, None]:
        """
        Executes a transform stage.

        Returns:
            t.Union[str, None]: Name of the created view.
        """
        src = self.defs
        name = src.name
        await self.idb.sql(f'CREATE OR REPLACE VIEW "{name}" as {src.query}')
        if src.show_schema:
            sch = c.connection.Schema(self.idb)
            fields: m.Columns = await sch.show(name)
            self.log.info(f"Schema of '{name}'\n{fields.print()}")
        if src.show:
            self.log.info(await self._show(src.show))
        return name


class SinkStageHandler(BaseStageHandler):
    """Handles execution for sink stages."""

    require_connection = True

    async def execute(self) -> t.Union[str, None]:
        """
        Executes a sink stage.

        Returns:
            t.Union[str, None]: None.
        """
        src = self.defs
        conn = self._connection
        await self.idb.inject_secrets(src.name, src.connection.config)
        if src.query:
            await self.idb.sql(src.query)
            await conn.sink(src.name)
        else:
            await conn.sink(self.block.prev)
        if src.show:
            self.log.info(
                dd(
                    f"{src.connection.kind} > {src.name}:\n{src.connection.locator}"
                )
            )
        return None


class SQLStageHandler(BaseStageHandler):
    """Handles execution for SQL stages."""

    require_connection = True

    async def execute(self) -> t.Union[str, None]:
        """
        Executes an SQL stage.

        Returns:
            t.Union[str, None]: Name of the target object.
        """
        src = self.defs
        name = src.name
        conn = self._connection
        await self.idb.inject_secrets(src.name, src.connection.config)
        await conn.sql(src.query)
        if src.show:
            self.log.info(await self._show(src.show))
        return name


class InternalStageHandler(BaseStageHandler):
    """Handles execution for internal stages."""

    async def execute(self) -> t.Union[str, None]:
        """
        Executes an internal stage.

        Returns:
            t.Union[str, None]: Name of the created table.
        """
        src = self.defs
        name = src.name
        resp = await self.idb.sql(src.query)
        if src.show:
            try:
                df = await resp.df()
                await self.idb.register(f"_tmp_{name}", df)
                await self.idb.sql(
                    f'CREATE TABLE "{name}" AS SELECT * FROM "_tmp_{name}"'
                )
                self.log.info(await self._show(src.show))
            except AttributeError:
                self.log.info(
                    dd(f"{src.kind} > {src.name}:\nNothing to show")
                )
        return name


class StageFactory:
    """Factory to obtain the appropriate stage handler based on stage kind."""

    # TODO: consider  register_handler classmethod
    HANDLERS = {
        "pipeline": PipelineStageHandler,
        "source": TapStageHandler,
        "tap": TapStageHandler,
        "transform": TransformStageHandler,
        "sink": SinkStageHandler,
        "sql": SQLStageHandler,
        "internal": InternalStageHandler,
        "self": InternalStageHandler,
    }

    @staticmethod
    async def get_handler(block: "StageBlock") -> BaseStageHandler:
        """
        Creates a stage handler instance.

        Args:
           block ("StageBlock"): The datablock instance.

        Returns:
            BaseStageHandler: The corresponding stage handler.
        """
        try:
            kind = block.defs.kind.lower()
            hndlr = StageFactory.HANDLERS[kind]
            return hndlr(block)
        except (KeyError, TypeError):
            raise ValueError(f"Unknown stage kind: {kind!r}")


class StageBlock:
    """Executable piece of a pipeline."""

    def __init__(
        self,
        conn: "DDB",
        defs: m.Stage,
        context: m.FlowContext,
        default_connection: m.ConnectionConfiguration,
        variables: t.Optional[m.Variables] = None,
        logger: t.Optional[logging.Logger] = None,
        prevous_stage: t.Optional[str] = None,
        log_context: t.Optional[str] = None,
    ) -> None:
        """
        Args:
            conn (DDB): DuckDB connection.
            defs (m.Stage): Stage definition.
            context (m.FlowContext): Context object.
            default_connection (m.ConnectionConfiguration): Global persistent
                connection configuration.
            variables (m.Variables, optional): Any variables passed to pipeline.
                Defaults to {}.
            logger (logging.Logger, optional): If set then messages are sent to
                this logger. Defaults to None.
            prevous_stage (str, optional): Reference to previous stage, used in
                sink to obtain data from. Defaults to None.
            log_context (str, optional): Additional logging context.
                Defaults to None.
        """
        self.idb = conn
        self.log = logger or logging.getLogger(__name__)
        self.vars = m.Variables() if variables is None else variables
        self.default_connection = default_connection
        self.ctx = context
        self.defs = defs
        self.prev = prevous_stage
        self.log_context = log_context

    def prepare(self, src: m.Stage) -> m.Stage:
        """
        Prepare dynamic variables in the block.

        Args:
            src (m.Stage): Current block.

        Returns:
            m.Datablock: Block with variables evaluated.
        """
        self.renderer = Renderer(context=self.ctx, API=API, variables=self.vars)
        if src.skip_if:
            src.skip_if = string_to_bool(self.render(src.skip_if))
        if src.query:
            src.query = self.render(src.query)
        if src.connection:
            src.connection = self.render(src.connection)
        return src

    def render(self, templ: t.Union[str, BaseModel]) -> str:
        """
        Evaluates a template string using context and API.

        Available variables in the template:
          - context: Context object.
          - API: API object.
          - variables: Variables dictionary.

        Args:
            templ (t.Union[str, m.BaseModel]): Template containing variables.

        Returns:
            str: Evaluated query string.
        """
        try:
            cls = None
            if isinstance(templ, BaseModel):
                cls = type(templ)
                templ = templ.model_dump(mode="json")  # type: ignore[assignment]
            out = self.renderer.render(templ)  # type: ignore[assignment]
            if cls is not None:
                out = cls.model_validate(out)
            return out  # type: ignore
        except Exception as e:
            self.log.error(templ)
            self.log.error(e)
            raise

    async def do(self) -> t.Union[str, None]:
        """
        Executes the current block.

        Returns:
            t.Union[str, None]: Executed block name.
        """
        self.defs = self.prepare(self.defs)
        if bool(self.defs.skip_if):
            self.log.info(f"Skip '{self.defs.name}'")
            return None
        handler = await StageFactory.get_handler(self)
        await handler._init_connection()
        return await handler.execute()

    async def show_schema(self) -> t.Optional[m.core.SchemaItem]:
        self.defs = self.prepare(self.defs)
        if bool(self.defs.skip_if):
            self.log.info(f"Skip '{self.defs.name}' (schema mode)")
            return None

        handler = await StageFactory.get_handler(self)
        await handler._init_connection()
        return await handler.show_schema()


# ------------------------------------------------------------------------------
# AsyncFlow Class (Public Interface)
# ------------------------------------------------------------------------------


class AsyncFlow:
    """Controls the flow of data based on the pipeline definition.

    The stages are run in the defined order and can either fetch data
    from a source (tap), transform via SQL, or store to a sink.
    """

    def __init__(
        self,
        defs: m.Stages,
        context: m.FlowContext,
        default_connection: m.ConnectionConfiguration,
        variables: m.Variables = None,
        logger: logging.Logger = None,
        conn: "DDB" = None,
        flow_control: "FlowControl" = None,
        log_context: str = None,
    ) -> None:
        """
        Args:
            defs (m.Stages): List of stages to be processed.
            context (m.FlowContext): Dynamic context for query templates.
            default_connection (m.ConnectionConfiguration): Configuration passed
                to underlying storage or database.
            variables (m.Variables, optional): Variables for tap or prepare.
                Defaults to {}.
            logger (logging.Logger, optional): Logger for show() requests.
                Defaults to None.
            conn (DDB, optional): Existing DuckDB connection. If not set, a new
                connection is created.
            flow_control (FlowControl, optional): Flow control configuration.
                Defaults to FlowControl().
            log_context (str, optional): Log context passed to each stage.
                Defaults to None.
        """
        self.defs = defs
        self.vars = m.Variables() if variables is None else variables
        self.lastname: t.Optional[str] = None
        self.ctx: m.FlowContext = context
        self.conn_opts: m.ConnectionConfiguration = default_connection
        self.logger = logger
        self.flow_control = flow_control or FlowControl()
        self.log_context = log_context
        self.idb: t.Optional["DDB"] = conn

    @property
    def logs(self) -> t.List[str]:
        """Returns the log buffer."""
        pass

    @property
    def log(self) -> logging.Logger:
        """Returns the logger."""
        if self.logger:
            return self.logger
        return null_logger()

    async def connect(self) -> None:
        """
        Connects to the underlying database if not already connected.
        """
        if self.idb:
            return
        else:
            self.idb = await DDB.connect(self.conn_opts)

    async def run(self) -> "AsyncFlow":
        """
        Runs the pipeline stages.

        Returns:
            AsyncFlow: Self instance after running the pipeline.
        """
        if not self.idb:
            await self.connect()
        start = arrow.get()

        vars_info = {k: v for k, v in self.vars.items()}
        self.log.info(f"Pipeline called with variables:\n{vars_info}")
        for step in self.defs.steps():
            try:
                if step.kind == "header":
                    continue
                if step.log_level:
                    self.log.setLevel(step.log_level.value)
                log_ctx = f"[{self.log_context}]" if self.log_context else ""
                self.log.info(f"{step.kind} '{step.name}' {log_ctx}")
                stage = StageBlock(
                    self.idb,  # type: ignore[assignment]
                    step,
                    self.ctx,
                    self.conn_opts,
                    variables=self.vars,
                    logger=self.log,
                    prevous_stage=self.lastname,  # type: ignore[assignment]
                    log_context=self.log_context,
                )
                self.lastname = await stage.do() or self.lastname
            except Exception as ex:
                msg = dd(f"{ex.__class__.__name__} in {step.name}: {ex}")
                if step.on_error == FlowControl.ON_ERROR_FAIL:
                    if self.flow_control.on_error == FlowControl.ON_ERROR_FAIL:
                        self.log.error(
                            f"Pipeline failed at '{step.name}':\n{dd(msg)}"
                        )
                        end = arrow.get()
                        self.log.info(f"Run duration: {end - start}")
                        raise e.FlowRunError(
                            f"Failed at '{step.name}':\n{dd(msg)}"
                        )
                else:
                    self.log.warning(f"Failed '{step.name}':\n{dd(msg)}")
            if step.throttle and step.throttle > 0:
                self.log.info(f"Flow throttling {step.throttle}s")
                await sleep(step.throttle)
        end = arrow.get()
        self.log.setLevel(logging.INFO)
        self.log.info(f"Run duration: {end - start}")
        return self

    async def df(self) -> pd.DataFrame:
        """
        Returns a dataframe from the last stage.

        Returns:
            pd.DataFrame: Data from the last stage.
        """
        if not self.lastname:
            return pd.DataFrame()
        coro = await self.idb.sql(f'SELECT * FROM "{self.lastname}"')
        return await coro.df()

    async def show_schema(self) -> t.List[m.core.SchemaItem]:
        """
        Returns schemas for all supported stages.

        Returns:
            t.List[m.models.SchemaItem]: List of schema items.
        """
        items: t.List[m.core.SchemaItem] = []
        for step in self.defs:
            try:
                db = StageBlock(
                    self.idb,
                    step,
                    self.ctx,
                    self.conn_opts,
                    variables=self.vars,
                    logger=self.log,
                    prevous_stage=self.lastname,
                )
                st = await db.show_schema()
                if st is None:
                    continue
                elif isinstance(st, list):
                    items.extend(st)
                elif isinstance(st, m.core.SchemaItem):
                    items.append(st)
                else:
                    self.log.warning("Unexpected schema item type")
            except NotImplementedError:
                continue
            except Exception:
                raise
        return items

    async def pull_df(self) -> pd.DataFrame:
        """
        Convenience method to pull the final dataframe.

        Returns:
            pd.DataFrame: Data from the last stage.
        """
        await self.run()
        return await self.df()


class Flow:
    """Flow controls the flow of data based on the pipeline definition.
    The stages are run in the defined order and can either fetch data
    from source (tap), transform via SQL statement, or store to sink.

    This is the sync version and should be used in most cases.
    """

    def __init__(
        self,
        defs: m.Stages,
        context: m.FlowContext,
        default_connection: m.ConnectionConfiguration,
        variables: m.Variables = None,
        logger: logging.Logger = None,
        conn: duckdb.DuckDBPyConnection = None,
        flow_control: FlowControl = None,
        log_context: str = None,
    ) -> None:
        """
        Args:
            defs (Stages): List of stages to be processed.
            context (FlowContext): dynamic context that can be used
                in query templates.
            default_connection (ConnectionConfiguration): Configuration
                passed to underlying storage or database
                (s3 bucket, database connection string &c).
            variables (dict, optional): Any variables that can be referenced
                by a tap or prepare. Ony data structures that can be passed
                to pl.from_dicts() are supported in taps. Defaults to {}.
            logger (Logger, optional): If set then show() requests are logged.
                Defaults to None.
            conn (DuckDBPyConnection): Existing DuckDB connection. If not set
                new connection will be created.
        """
        self.flow = AsyncFlow(
            defs,
            context,
            default_connection,
            variables=m.Variables() if variables is None else variables,
            logger=logger,
            conn=conn,
            flow_control=flow_control or FlowControl(),
            log_context=log_context,
        )

    @property
    def logs(self):
        return self.flow.logs

    @property
    def log(self):
        return self.flow.log

    def connect(self):
        return asyncio_run(self.flow.connect())

    def run(self):
        asyncio_run(self.flow.run())
        return self

    def df(self):
        """Returns dataframe from the last stage"""
        return asyncio_run(self.flow.df())

    def show_schema(self) -> t.List[m.core.SchemaItem]:
        """
        Returns schemas of all supported stages
        """
        return asyncio_run(self.flow.show_schema())

    def pull_df(self) -> pd.DataFrame:
        """
        Convenience method to pull final dataframe

        Returns:
            pl.DataFrame: data from last stage
        """
        asyncio_run(self.flow.run())
        return asyncio_run(self.flow.df())
