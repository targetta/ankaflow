import typing as t
import unittest
from unittest.mock import AsyncMock, MagicMock

# Import your actual production models and types
from ..connections.rest.rest import (
    Page,
    CursorPaginationHandler,
    RestResponse,
)
from ..models.enums import DataType
from ..models.rest import (
    Request,
    CursorPaginator,
    CursorRequestDisposition,
    CursorResponseLocator,
    ResponseHandlerTypes,
)


def create_mock_rest_response(
    json_data: t.Dict[str, t.Any], headers: t.Optional[t.Dict[str, str]] = None
) -> RestResponse:
    """Creates a type-safe mock of RestResponse for unit tests."""
    mock_resp = MagicMock(spec=RestResponse)
    mock_resp.json = AsyncMock(return_value=json_data)
    mock_resp.headers = headers or {}
    return t.cast(RestResponse, mock_resp)


class TestCursorPaginationHandler(unittest.IsolatedAsyncioTestCase):
    """Unit tests specifically covering CursorPaginationHandler behavior."""

    def setUp(self):
        # Configure CursorPaginator model
        self.paginator_cfg = MagicMock(spec=CursorPaginator)
        self.paginator_cfg.kind = ResponseHandlerTypes.CURSOR_PAGINATOR
        self.paginator_cfg.cursor_locator = CursorResponseLocator.BODY
        self.paginator_cfg.cursor_param = "next_cursor"
        self.paginator_cfg.cursor_disposition = CursorRequestDisposition.QUERY
        self.paginator_cfg.cursor_name = "starting_after"
        self.paginator_cfg.stop_value = None

        # Setup mock RestResponse configuration
        self.res_config = MagicMock()
        self.res_config.handler = self.paginator_cfg
        self.res_config.locator = "items"
        self.res_config.content_type = DataType.JSON

        # Setup Request object
        self.initial_req = MagicMock(spec=Request)
        self.initial_req.endpoint = "/v1/items"
        self.initial_req.query = {}
        self.initial_req.body = None
        self.initial_req.response = self.res_config

        # Enable model_copy behavior on request mock
        def mock_copy(deep=True):
            cloned = MagicMock(spec=Request)
            cloned.endpoint = self.initial_req.endpoint
            cloned.query = dict(self.initial_req.query)
            cloned.body = (
                dict(self.initial_req.body)
                if isinstance(self.initial_req.body, dict)
                else self.initial_req.body
            )
            cloned.response = self.initial_req.response
            return cloned

        self.initial_req.model_copy.side_effect = mock_copy

        # Initialize Handler with mocked logger
        self.handler = CursorPaginationHandler(
            self.initial_req, id="test_req_1", logger=MagicMock()
        )
        self.handler.log = MagicMock()

    async def test_body_cursor_extraction_and_query_injection(self):
        mock_resp = create_mock_rest_response({
            "items": [{"id": 1}, {"id": 2}],
            "next_cursor": "xi",
        })

        page: Page = await self.handler.next(mock_resp)

        self.assertEqual(len(page.data), 2)
        self.assertIsNotNone(page.next_request)
        self.assertEqual(
            page.next_request.query["starting_after"], "xi" # type: ignore
        )

    async def test_body_cursor_injection_into_none_body(self):
        self.paginator_cfg.cursor_disposition = CursorRequestDisposition.BODY
        self.paginator_cfg.cursor_name = "meta.cursor"
        self.initial_req.body = None

        mock_resp = create_mock_rest_response({
            "items": [{"id": 10}],
            "next_cursor": "zoo",
        })

        page: Page = await self.handler.next(mock_resp)

        self.assertIsNotNone(page.next_request)
        self.assertEqual(page.next_request.body["meta"]["cursor"], "zoo") # type: ignore

    async def test_header_cursor_extraction(self):
        self.paginator_cfg.cursor_locator = CursorResponseLocator.HEADER
        self.paginator_cfg.cursor_param = "X-Next-Cursor"

        mock_resp = create_mock_rest_response(
            {"items": [{"id": 1}]}, headers={"X-Next-Cursor": "fauna"}
        )

        page: Page = await self.handler.next(mock_resp)

        self.assertIsNotNone(page.next_request)
        self.assertEqual(
            page.next_request.query["starting_after"], "fauna" # type: ignore
        )

    async def test_rfc8288_url_extraction(self):
        self.paginator_cfg.cursor_locator = CursorResponseLocator.RFC_URL

        headers = {
            "Link": '<https://api.example.com/v1/items?starting_after=page_2&limit=50>; rel="next"'  # noqa: E501
        }
        mock_resp = create_mock_rest_response(
            {"items": [{"id": 1}]}, headers=headers
        )

        page: Page = await self.handler.next(mock_resp)

        self.assertIsNotNone(page.next_request)
        self.assertEqual(page.next_request.endpoint, "/v1/items") # type: ignore
        self.assertEqual(page.next_request.query["starting_after"], "page_2") # type: ignore
        self.assertEqual(page.next_request.query["limit"], "50") # type: ignore

    async def test_stop_value_sentinel_ends_pagination(self):
        self.paginator_cfg.stop_value = "END_OF_DATA"

        mock_resp = create_mock_rest_response({
            "items": [{"id": 1}],
            "next_cursor": "END_OF_DATA",
        })

        page: Page = await self.handler.next(mock_resp)

        self.assertEqual(len(page.data), 1)
        self.assertIsNone(page.next_request)

    async def test_empty_cursor_returns_none_next_req(self):
        mock_resp = create_mock_rest_response({
            "items": [{"id": 1}],
            "next_cursor": "",
        })

        page: Page = await self.handler.next(mock_resp)

        self.assertIsNone(page.next_request)

    async def test_duplicate_cursor_loop_detection(self):
        mock_resp = create_mock_rest_response({
            "items": [{"id": 1}],
            "next_cursor": "repeat_cursor",
        })

        # First pass succeeds
        page1: Page = await self.handler.next(mock_resp)
        self.assertIsNotNone(page1.next_request)

        # Second pass triggers loop prevention
        page2: Page = await self.handler.next(mock_resp)
        self.assertIsNone(page2.next_request)
        self.handler.log.warning.assert_called_once() # type: ignore


if __name__ == "__main__":
    unittest.main()
