import json
import logging
import yaml
from logging.handlers import MemoryHandler
import traceback

from ankaflow import AsyncFlow, FlowError, Stages, ConnectionConfiguration, FlowContext


console_handler = logging.StreamHandler()  # Sends to browser's JS console in Pyodide
console_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
console_handler.setFormatter(formatter)

# Root logger = for all internal modules using `logging.getLogger(__name__)`
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(console_handler)


def parse_yaml(text: str):
    try:
        return yaml.safe_load(text)
    except yaml.error.YAMLError as e:
        raise RuntimeError(str(e))


class BufferHandler(MemoryHandler):
    def logs(self) -> list[str]:
        return [f"{record.getMessage()}\n----\n" for record in self.buffer]


async def main(yaml_defs: str, env: dict = None) -> str:
    """
    Entry point for Pyodide worker.

    Args:
        yaml_defs (str): YAML pipeline definition string.
        env (dict): Optional environment/config variables.

    Returns:
        str: JSON-encoded log messages.
    """
    env = env or {}
    model_dict = parse_yaml(yaml_defs)
    defs = Stages.model_validate(model_dict)
    opts = ConnectionConfiguration(bucket="/tmp")
    ctx = FlowContext()

    buffer_handler = BufferHandler(1000, flushLevel=logging.DEBUG)
    buffer_handler.setFormatter(formatter)
    logger = logging.getLogger("ankaflow-web")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(buffer_handler)

    try:
        f = AsyncFlow(defs, ctx, opts, logger=logger)
        await f.run()
        return json.dumps(buffer_handler.logs())
    except FlowError:
        logger.error("FlowError occurred:\n" + traceback.format_exc())
        return json.dumps(buffer_handler.logs())
    except Exception:
        logger.error("Unhandled error:\n" + traceback.format_exc())
        return json.dumps(buffer_handler.logs())
