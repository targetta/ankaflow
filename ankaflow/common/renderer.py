import typing as t
from jinja2 import Environment, BaseLoader
import json
import logging

from .util import print_error

log = logging.getLogger(__name__)


class Renderer:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def render_string(
        self,
        string: str,
        squash_whitespace: bool = False,
        infer_type: bool = True,
    ) -> t.Any:
        if not isinstance(string, str):
            return string

        jenv = Environment(
            loader=BaseLoader,  # type: ignore
            variable_start_string="<<",
            variable_end_string=">>",
            block_start_string="<%",
            block_end_string="%>",
            comment_start_string="<#",
            comment_end_string="#>",
        )

        # Register filters
        jenv.filters["bool"] = lambda v: bool(v)
        jenv.filters["int"] = lambda v: int(v)
        jenv.filters["float"] = lambda v: float(v)

        tmpl = jenv.from_string(string)
        rendered = tmpl.render(**self.kwargs).strip()

        if squash_whitespace:
            rendered = " ".join(rendered.split())

        # Type inference
        # Make the template return proper type
        # rather than string representation.
        if infer_type:
            lowered = rendered.lower()
            if lowered == "true":
                return True
            elif lowered == "false":
                return False
            elif lowered in {"null", "none"}:
                return None
            try:
                return int(rendered)
            except ValueError:
                pass
            try:
                return float(rendered)
            except ValueError:
                pass

        return rendered

    def render(self, input: t.Union[str, dict, list]) -> t.Any:
        try:
            if isinstance(input, str):
                if input.startswith("@json"):
                    rendered = self.render_string(
                        input[5:], squash_whitespace=True, infer_type=False
                    )
                    return json.loads(rendered)

                if input.startswith("JSON>"):
                    log.warning(
                        "Using deprecated JSON> prefix. Please replace with '@json'."  # noqa:E501
                    )
                    rendered = self.render_string(
                        input[5:], squash_whitespace=True, infer_type=False
                    )
                    return json.loads(rendered)

                return self.render_string(input)

            if isinstance(input, dict):
                return {k: self.render(v) for k, v in input.items()}

            if isinstance(input, list):
                return [self.render(i) for i in input]

            return input

        except Exception as e:
            raise ValueError(
                print_error(f"Cannot render {type(input)}:", str(e))
            )
