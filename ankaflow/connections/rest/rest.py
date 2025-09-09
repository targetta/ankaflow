# type: ignore[assignment]
import typing as t
import logging
from asyncio import sleep
from shortuuid import ShortUUID
from tempfile import gettempdir
from pathlib import Path
import jmespath
import sys

from ...models.connections import RestConnection
from ...models import rest as rst
from ...models import enums
from .common import MaterializerProtocol, Materializer, MaterializeError
from ..connection import Connection
from ...common.util import null_logger, print_error

IS_PYODIDE = sys.platform == "emscripten"

if IS_PYODIDE:
    from .browser import RestClient
    from .browser import RestResponse
else:
    from .server import RestClient
    from .server import RestResponse


log = logging.getLogger("buimatic")


class ResponseHandler:
    def __init__(self, request: rst.Request, id: str,
                 logger: logging.Logger = None):
        self.id = id
        self.req = request
        self.res = request.response
        self._setup()
        self.log = logger or null_logger()

    def _setup(self):
        # Child classes should use this method
        # to set up their internals
        pass

    async def read_response(self, resp: RestResponse):
        if self.res.content_type == enums.DataType.JSON:
            data = await resp.json()
            if self.res.locator:
                return jmespath.search(self.res.locator, data)
            return data


class PaginationHandler(ResponseHandler):
    class Page:
        # Output structure including page data and next page request
        def __init__(self, data: t.List[t.Any], next_request: rst.Request):
            self.data = data
            self.next_request = next_request

    def _setup(self):
        if self.res.handler.param_locator == enums.ParameterDisposition.QUERY:
            self.current_page = self.req.query[self.res.handler.page_param]
        else:
            self.current_page = self.req.body[self.res.handler.page_param]

        self.init_val = self.current_page
        # Total records received since page 1
        self._received_count: int = 0
        # Records from response
        self._records = []
        self._data = t.Any

    def has_next(self):
        # When api reports total records use it
        if self.res.handler.total_records:
            total_count = (
                int(jmespath.search(self.res.handler.total_records, self._data))
                or 0
            )  # noqa:E501
            if total_count <= self._received_count:
                return False
            else:
                return True
        # If not we have to set it
        elif len(self._records) < self.res.handler.page_size:
            return False
        else:
            return True

    async def read_response(self):  # type: ignore
        if self.res.content_type == enums.DataType.JSON:
            self._data = await self.resp.json()
            if self.res.locator:
                self._records = jmespath.search(self.res.locator, self._data)  # noqa:E501
            else:
                self._records = self._data
            self._received_count += len(self._records)
        else:
            raise NotImplementedError("Content type not implemented")

    def update_request(self):
        # Calculate next page number and return modified request
        if not self.has_next():
            return None
        next_req = self.req.model_copy(deep=True)
        self.current_page = int(self.current_page) + int(
            self.res.handler.increment
        )  #  noqa:E501
        if self.res.handler.param_locator == enums.ParameterDisposition.QUERY:
            next_req.query[self.res.handler.page_param] = self.current_page
        else:
            next_req.body[self.res.handler.page_param] = self.current_page
        return next_req

    async def next(self, response: RestResponse):
        self.resp = response
        await self.read_response()
        next_req = self.update_request()
        self.log.debug(f"Next request:\n{next_req}")
        return PaginationHandler.Page(self._records, next_req)


class URLPollingHandler(ResponseHandler):
    """
    URLPoller class retrieves raw data from url supplied by response
    """

    def _setup(self):
        self.tmppath = Path(gettempdir(), self.id)
        self.wait = 0

    async def poll(self, response: RestResponse):
        data = await response.json()
        await sleep(self.wait)
        url = jmespath.search(self.res.locator, data)
        completed = True
        if self.res.handler.ready_status:
            completed = jmespath.search(self.res.handler.ready_status, data)
            self.log.debug(
            f"Waiting for '{self.res.handler.ready_status}', current: {completed}"  # noqa: E501
            )
        self.wait = self.wait * 1.5 if self.wait else 1
        return (url, completed)

    async def stream(self, client: RestClient, url: str):
        return await client.stream(url, self.tmppath)


class StatePollingHandler(ResponseHandler):
    def _setup(self):
        self.wait = 0

    async def poll(self, response: RestResponse):
        resp = await response.json()
        # if jmespath.search(self.res.handler.ready_status, data):
        #     return jmespath.search(self.res.locator, data)
        #         sleep(self.wait)
        completed = False
        completed = jmespath.search(self.res.handler.ready_status, resp)
        self.log.debug(
            f"Waiting for '{self.res.handler.ready_status}', current: {completed}"  # noqa: E501
        )
        self.wait = self.wait * 1.5 if self.wait else 1
        if completed:
            data = jmespath.search(self.res.locator, resp)
            self.log.debug(
                f"Polling complete, data at '{self.res.locator}': {bool(data)}"
            )
            return (data, completed)
        else:
            self.log.debug(resp)
            self.wait = self.wait * 1.5 if self.wait else 1
            await sleep(self.wait)
            return (None, completed)


class RestApi:
    def __init__(
        self,
        client: rst.RestClientConfig,
        materializer: MaterializerProtocol,
        logger: logging.Logger = None,
    ):
        """
        Creates new REST API.

        Args:
            client (m.RestClientConfig): Client configuration
            connection (DuckDBPyConnection, optional): Connection used to
                materialize results. Defaults to None.
            table (str, optional): Table name to materialize data into.
                Defaults to None.
        """
        # ID is used to create temporary relations
        self.id = ShortUUID().uuid()
        self.confg = client
        self._client: RestClient = None
        self.mat = materializer
        self.log = logger or null_logger()

    def create_client(self):
        if self._client:
            # reopen client if disconnected
            if self._client.closed:
                self._client.connect()
            return
        self._client = RestClient(self.confg).connect()

    async def handle_response(self, resp: RestResponse):
        # TODO: Rework the handling logic
        # Each handler should deal with their own whiles
        # THis handles should only load correct handler
        # and pass control over
        # hndlr = SpecificResponseHandler(client, materializer)
        # hndlr.handle_response()
        if not self.res.handler:
            data = await ResponseHandler(self.req, self.id).read_response(resp)
            await self.mat.materialize(data)
            return
        if self.res.handler.kind == rst.ResponseHandlerTypes.STATEPOLLING:
            poller = StatePollingHandler(self.req, self.id, logger=self.log)
            self.log.info("Start state polling")
            while True:
                data_status = await poller.poll(resp)
                if data_status[1]:
                    await self.mat.materialize(data_status[0])
                    break
        # URL Polling
        elif self.res.handler.kind == rst.ResponseHandlerTypes.URLPOLLING:
            poller = URLPollingHandler(self.req, self.id)
            self.log.info("Start URL polling")
            while True:
                url_status = await poller.poll(resp)
                if url_status[1]:
                    break
                self.log.debug(self.req)
                resp = await self._client.fetch(self.req)
            # TODO: Path handling is clunky and does not make really sense
            if url_status[0]:
                path = await poller.stream(self._client, url_status[0])
            else:
                # No data was fetched
                # Ensure the path exists
                open(poller.tmppath, "a").close()
                path = poller.tmppath
            await self.mat.materialize(None, filename=path)
        # Paginator
        elif self.res.handler.kind == rst.ResponseHandlerTypes.PAGINATOR:
            paginator = PaginationHandler(self.req, self.id)
            while True:
                page = await paginator.next(resp)
                data = page.data
                await self.mat.materialize(data)
                next_req = page.next_request
                if not next_req:
                    break
                self.log.debug(next_req)
                if self.res.handler.throttle and self.res.handler.throttle > 0:
                    self.log.debug(f"Throttling {self.res.handler.throttle}s")
                    await sleep(self.res.handler.throttle)
                resp = await self._client.fetch(next_req)
        # Single page reponse
        else:
            data = await ResponseHandler(self.req, self.id).read_response(resp)
            await self.mat.materialize(data)

    async def fetch(self, request: rst.Request):
        self.req = request
        self.res = request.response
        self.create_client()
        try:
            self.log.debug(self.req)
            resp = await self._client.fetch(self.req)
            await self.handle_response(resp)
        except Exception as e:
            log.exception(e)
            raise
        finally:
            self._client.disconnect()


class Rest(Connection):
    def init(self):
        self.conn = t.cast(RestConnection, self.conn)
        mat = Materializer(
            self.c,
            self.conn.request.response.content_type,
            self.name,
            self.schema_,
            self.conn.fields,
        )
        self.api = RestApi(self.conn.client, mat, logger=self.log)
        self.request = self.conn.request
        self.log = self.log

    def parse_config(self, config: dict):
        pass

    async def show_schema(self, tap: bool = False):
        # We need to tap before schema can be shown
        # Unless the schema has already been inferred
        if tap:
            await self.tap()
        return await self.schema_.show(self.name)

    async def tap(self, query: str = None, limit: int = None):
        try:
            await self.api.fetch(self.request)
        except MaterializeError as e:
            # This happens when pages have different structure
            # We try to create the table before
            msg = [
                e,
                "Should supply explicit schema",
                "Inferred schema:",
                await self.show_schema(tap=False),
            ]
            self.log.warning(print_error(*msg))
            raise
        except Exception:
            raise

    async def sink(self, from_name: str = None):
        await self.api.fetch(self.request)
