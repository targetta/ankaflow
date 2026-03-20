import typing as t
import json
import logging

from .util import print_error
from .security import StrictEnvironment, secure_context, jinja_sanitize

log = logging.getLogger(__name__)


class Renderer:
    def __init__(
        self,
        **kwargs
    ):
        self.kwargs = kwargs

    def render_string(
        self,
        string: str,
        squash_whitespace: bool = False,
        infer_type: bool = True,
    ) -> t.Any:
        if not isinstance(string, str):
            return string
        safe_kwargs = {}
        for k in self.kwargs:
            if k.startswith("__"):
                safe_kwargs[k] = None
            else:
                safe_kwargs[k] = jinja_sanitize(self.kwargs[k])

        with secure_context():
            env = StrictEnvironment(
                variable_start_string="<<",
                variable_end_string=">>",
                block_start_string="<%",
                block_end_string="%>",
                comment_start_string="<#",
                comment_end_string="#>",
            )
            # Register filters
            env.filters["bool"] = lambda v: bool(v)
            env.filters["int"] = lambda v: int(v)
            env.filters["float"] = lambda v: float(v)

            tmpl = env.from_string(string)
            rendered = tmpl.render(**safe_kwargs).strip()

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
