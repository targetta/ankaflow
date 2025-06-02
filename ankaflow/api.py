from datetime import date, datetime
import typing as t
import arrow
import jmespath
import pandas as pd


class UserGeneratedError(Exception):
    pass


class API:
    # Need to cast numpy types to python types
    # numpy types are not supported in jinja templates?
    @staticmethod
    def int(input: t.Any):
        return int(input)

    @staticmethod
    def dt(
        datelike: t.Union[
            str, "int", float, date, datetime, arrow.Arrow, None
        ] = None,
        tz: str | None = None,
        format: str | None = None,
        default: str | None = None,
    ):
        """
        Attempts to convert input object into Arrow.

        Args:
            datelike (t.Union[str, int, float, date, datetime, arrow.Arrow],
                optional): Object to be evaluated to Arrow. Defaults to None.
            tz (str, optional): IANA timezone string If . Defaults to None.
            format (str, optional): If set use this format to parse input.
                Defaults to None.
            default (str, optional): Use default timestamp if first attempt
                fails. Defaults to None.

        Returns:
            Arrow: Arrow object
        """
        # Handle various return types:
        # nan - when table exists but not data
        # nanosecond timestamp in bigquery
        if pd.isna(datelike):  # type: ignore
            datelike = default
        if isinstance(datelike, (int)):
            if datelike > 9999999999:
                return arrow.get(pd.to_datetime(datelike))
        try:
            if datelike is None:
                a = arrow.get()
            elif format:
                a = arrow.get(datelike, format)  # type: ignore
            else:
                a = arrow.get(datelike)
        except Exception:
            if default:
                a = arrow.get(default)
            else:
                raise
        if tz:
            return a.replace(tzinfo=tz)
        return a

    @staticmethod
    def look(
        lookup: str, data: t.Any, default: t.Any = None
    ) -> t.Union[str, None]:
        """
        Extracts value from given structure, or default value
        if requested value not found.

        Args:
            lookup (str): Lookup query
            data (t.Any): Iterable object
            default (t.Any, optional): Default value to return.
                Defaults to None.

        Returns:
            t.Union[str, None]: _description_
        """
        found = jmespath.search(lookup, data)
        if found is None:
            return default
        return found

    @staticmethod
    def peek(value: t.Any) -> str:
        """
        Returns information about value type:
        module.Class
        Useful for debugging data obtained from
        remote sources.

        Args:
            value (Any): Any value

        Returns:
            str: _description_
        """
        m = value.__class__.__module__
        n = value.__class__.__name__
        return f"{m}.{n}"

    @staticmethod
    def sqltuple(iterable: t.Iterable, mode: str = "string"):
        """
        Returns SQL tuple literal from given iterable.

        Args:
            iterable (Iterable): Any iterable.
            mode (str, optional): "string"|"number". Defaults to "string".

        Raises:
            NotImplementedError: When unsupported mode is used

        Returns:
            str: SQL tuple literal
        """
        items = [str(it) for it in iterable]
        if mode == "string":
            result = "','".join(items)
            return f"('{result}')"
        elif mode == "number":
            result = "','".join(items)
            return f"('{result}')"
        else:
            raise NotImplementedError(f"Invalid mode: {mode}")

    @staticmethod
    def setvariable(collection: dict, key: str, value: t.Any) -> t.Any:
        """
        Attempts to assign value to a dictionary under the given key.

        Args:
            collection (dict): Dictionary to use e.g. variables
            key (str): Key to store thalue to be storese value under
            value (t.Any): V

        Returns:
            t.Any: Original value
        """
        collection[key] = value
        return value

    @staticmethod
    def error(expression: t.Any, message: str) -> t.Any:
        """
        Raises an exception if input expression evaluates
        to True.

        Args:
            expression (t.Any): Expression to be evaluetd
            message (str): Message to include in exception

        Raises:
            UserGeneratedError: _description_

        Returns:
            t.Any: Expression
        """
        if expression:
            raise UserGeneratedError(message)
        return expression
