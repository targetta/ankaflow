import typing as t
from sqlglot import parse_one
import pandas as pd
import logging
import json
from io import StringIO
from pathlib import Path

from .connection import Connection
from .. import models as m
from ..internal import CatalogException

log = logging.getLogger(__name__)


class Variable(Connection):
    async def tap(self, query: t.Optional[str] = None, limit: int = 0):
        try:
            var = self.vars[self.conn.locator]
        except KeyError:
            var = None
        var_str = json.dumps(var)
        await self.c.read_json(StringIO(var_str), self.name, {})

    async def sink(self, from_name: str):
        rel = await self.c.sql(f'SELECT * FROM "{from_name}"')
        df = await rel.df()
        self.vars[self.conn.locator] = df.to_dict(orient="records")


class Parquet(Connection):
    async def tap(self, query: t.Optional[str] = None, limit: int = 0):
        if self.conn.raw_dispatch:
            if not query:
                raise ValueError("Query is mandatory")
            rewritten = self._raw_sql_rewriter(query)
            stmt = f"""
                CREATE TABLE "{self.name}"
                AS
                {rewritten}
                """.strip()
        else:
            path = self.locate(use_wildcard=True)
            stmt = f"""
                CREATE OR REPLACE TABLE
                "{self.name}"
                AS SELECT
                {query or "*"}
                FROM read_parquet('{path}', union_by_name = true)
            """
            if limit:
                stmt = parse_one(stmt).subquery().limit(limit).sql()  # type: ignore[attr-defined]
        try:
            await self.c.sql(stmt)
        except Exception as e:
            log.exception(e)
            raise

    async def sink(self, from_name: str):
        path = self.locate()
        stmt = f"""
        COPY (
            SELECT * FROM "{from_name}"
        ) TO '{path}' (FORMAT PARQUET);
        """
        await self.c.sql(stmt)

    async def show_schema(self) -> m.Columns:
        table_name = f"schema_{self.name}"
        try:
            return await self.schema_.show(self.name)
        except CatalogException:
            pass
        try:
            path = self.locate(use_wildcard=True)
            await self.c.sql(f"""
                CREATE OR REPLACE TABLE "{table_name}" AS
                SELECT * FROM read_parquet('{path}', union_by_name = true) LIMIT 1
            """)  # noqa: E501
            return await self.schema_.show(table_name)
        except Exception as e:
            return self.schema_.error(e)


class JSON(Connection):
    async def tap(self, query: t.Optional[str] = None, limit: int = 0):
        path = self.locate(use_wildcard=True)
        stmt = f"""
        CREATE OR REPLACE TABLE "{self.name}"
        AS
        SELECT
        {query or "*"}
        FROM read_json_auto('{path}')
        """
        if limit:
            stmt = parse_one(stmt).subquery().limit(limit).sql()  # type: ignore[attr-defined]
        await self.c.sql(stmt)

    async def sink(self, from_name: str):
        path = self.locate()
        stmt = f"""
        COPY "{from_name}" TO '{path}' (FORMAT JSON);
        """
        await self.c.sql(stmt)

    async def show_schema(self) -> m.Columns:
        try:
            return await self.schema_.show(self.name)
        except CatalogException:
            pass
        try:
            await self.tap(query="*", limit=1)
            return await self.schema_.show(self.name)
        except Exception as err:
            return self.schema_.error(err)


class CSV(Connection):
    async def tap(self, query: t.Optional[str] = None, limit: int = 0):
        path = self.locate(use_wildcard=True)
        stmt = f"""
        CREATE OR REPLACE TABLE "{self.name}"
        AS
        SELECT
        {query or "*"}
        FROM read_csv('{path}')
        """
        if limit:
            # Cannot access attribute "subquery" for class "Expression"
            # Attribute "subquery" is unknown
            stmt = parse_one(stmt).subquery().limit(limit).sql()  # type: ignore[attr-defined]
        await self.c.sql(stmt)

    async def sink(self, from_name: str):
        path = self.locate()
        stmt = f"""
        COPY "{from_name}" TO '{path}' (FORMAT CSV);
        """
        await self.c.sql(stmt)

    async def show_schema(self) -> m.Columns:
        try:
            return await self.schema_.show(self.name)
        except CatalogException:
            pass
        try:
            await self.tap(query="*", limit=1)
            return await self.schema_.show(self.name)
        except Exception as err:
            return self.schema_.error(err)


class File(Connection):
    async def tap(self, query: str | None = None, limit: int = 0):
        try:
            name: str = self.ctx.FileName or self.conn.locator
            fn = self.locate(name, use_wildcard=True)
            opts: dict = (self.conn.params or {}).copy()
            ext: t.Optional[str] = None
            if "kind" in opts:
                ext = opts["kind"]
                del opts["kind"]
            kind: str = str(
                self.ctx.FileType or ext or Path(fn).suffix.lstrip(".").lower()
            )
            if kind.lower() == "parquet":
                df = pd.read_parquet(fn, **opts)
            elif kind.lower() == "xml":
                df = pd.read_xml(fn, **opts)
            elif kind.lower() == "txt":
                df = pd.read_table(fn, **opts)
            elif kind.lower() == "csv":
                df = pd.read_csv(fn, **opts)
            elif kind.lower() == "xlsx":
                df = pd.read_excel(fn, **opts)
            elif kind.lower() == "json":
                df = pd.read_json(fn, **opts)
            elif kind.lower() == "html":
                df = pd.read_html(fn, **opts)[0]
            else:
                raise NotImplementedError(f"File type {kind} not supported")
            if limit:
                df = df.head(limit)
        except NotImplementedError:
            raise
        except Exception as e:
            log.exception(e)
            self.log.error(e)
            raise

        try:
            await self.c.register("filedf", df)
            await self.c.sql(f"""
                CREATE TABLE "{self.name}" AS
                SELECT * FROM filedf
            """)
        finally:
            try:
                await self.c.unregister("filedf")
            except Exception as e:
                log.warning(e)

    async def show_schema(self) -> m.Columns:
        try:
            return await self.schema_.show(self.name)
        except CatalogException:
            pass
        try:
            await self.tap(query="*", limit=1)
            return await self.schema_.show(self.name)
        except Exception as err:
            return self.schema_.error(err)
