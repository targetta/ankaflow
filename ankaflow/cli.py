import argparse
import logging
import os
import sys
import traceback
from pathlib import Path

from ankaflow import (  # type: ignore
    ConnectionConfiguration,
    S3Config,
    GSConfig,
    BigQueryConfig,
    Stages,
    FlowContext,
    Flow,
    Variables,
)


def resolve_yaml_path(path_arg: str) -> Path:
    """Resolve YAML path, remapping 'DEMO' to a relative demo file path."""
    if path_arg.upper() == "DEMO":
        return Path(__file__).parent / "yaml" / "example.yaml"
    return Path(path_arg).resolve()


def parse_keyval(pairs: list[str]) -> dict[str, str]:
    result = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"Invalid format: '{pair}'. Expected key=value.")
        key, value = pair.split("=", 1)
        result[key] = value
    return result


def resolve_config() -> ConnectionConfiguration:
    s3 = S3Config(
        bucket=os.getenv("AWS_DEFAULT_BUCKET"),
        region=os.getenv("AWS_REGION"),
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    gs = GSConfig(
        bucket=os.getenv("GOOGLE_STORAGE_BUCKET"),
        region=os.getenv("GOOGLE_STORAGE_REGION"),
        hmac_key=os.getenv("GS_HMAC_KEY_ID"),
        hmac_secret=os.getenv("GS_HMAC_SECRET"),
        credential_file=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    )
    bigquery = BigQueryConfig(
        project=os.getenv("GOOGLE_CLOUD_PROJECT"),
        credential_file=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    )
    return ConnectionConfiguration(s3=s3, gs=gs, bigquery=bigquery)


def main():
    parser = argparse.ArgumentParser(
        description="Run a Ankaflow pipeline.\n\n"
        "S3, GS storage, and BigQuery connections can be automatically configured\n"  # noqa: E501
        "via environment variables. For Google Cloud Storage HMAC keys are required.\n"  # noqa: E501
        "See more: https://cloud.google.com/storage/docs/authentication/hmackeys\n"
        "\n"
        "Supported environment variables:\n"
        "\n"
        "S3 buckets:\n"
        "  AWS_DEFAULT_BUCKET          Default S3 bucket name\n"
        "  AWS_DEFAULT_REGION          S3 region\n"
        "  AWS_ACCESS_KEY_ID           S3 access key ID\n"
        "  AWS_SECRET_ACCESS_KEY       S3 secret access key\n"
        "\n"
        "GCS buckets:\n"
        "  GOOGLE_STORAGE_BUCKET       Default GCS bucket name\n"
        "  GOOGLE_STORAGE_REGION       GCS region\n"
        "  GS_HMAC_KEY_ID              GCS HMAC key ID\n"
        "  GS_HMAC_SECRET              GCS HMAC secret\n"
        "  GOOGLE_APPLICATION_CREDENTIALS Path to GCP credentials file (for writing Delta tables)\n"  # noqa: E501
        "\n"
        "Bigquery datasets:\n"
        "  GOOGLE_CLOUD_PROJECT        Google Cloud project ID\n"
        "  GOOGLE_APPLICATION_CREDENTIALS Path to GCP credentials file\n",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "yaml_path",
        help="Path to the pipeline YAML definition. Type DEMO to run demo flow.",  # noqa: E501
    )  # noqa: E501
    parser.add_argument(
        "-c", "--context", action="append", default=[], help="Context key=value"
    )
    parser.add_argument(
        "-v", "--variable", action="append", default=[], help="Variable key=value"  # noqa: E501
    )
    parser.add_argument("-l", "--log", help="Path to log file")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging (sets log level to DEBUG)",
    )
    parser.add_argument(
        "-o",
        "--output",
        nargs=2,
        metavar=("FORMAT", "PATH"),
        help="Write final dataframe to file",
    )

    args = parser.parse_args()

    # --- Logger setup ---
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    cli_log = logging.getLogger("ankaflow.cli")
    cli_log.setLevel(logging.INFO)
    cli_handler = logging.StreamHandler(sys.stdout)
    cli_handler.setFormatter(formatter)
    cli_log.addHandler(cli_handler)

    ankalog = logging.getLogger("ankalogger")
    ankalog.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    if args.log:
        file_handler = logging.FileHandler(args.log)
        file_handler.setFormatter(formatter)
        cli_log.addHandler(file_handler)
        ankalog.addHandler(file_handler)
    else:
        anka_handler = logging.StreamHandler(sys.stdout)
        anka_handler.setFormatter(formatter)
        ankalog.addHandler(anka_handler)

    # --- Load pipeline ---
    yaml_path = resolve_yaml_path(args.yaml_path)
    stages = Stages.load(yaml_path)

    # --- Setup context and variables ---
    contextdict = {
        "env": dict(os.environ),
        "user": parse_keyval(args.context),
    }
    context = FlowContext(contextdict)
    variables = parse_keyval(args.variable)
    config = resolve_config()
    vars = Variables(variables)
    # --- Run pipeline ---
    duct = Flow(
        stages,
        context,
        config,
        variables= vars,
        logger=ankalog,
    )
    try:
        duct.run()
    except Exception as e:
        ankalog.error(e)
    finally:
        ankalog.debug(vars)
    # --- Output final dataframe if requested ---
    if args.output:
        fmt, path = args.output
        df = duct.df()
        if df is None:
            cli_log.error("No dataframe returned. Cannot write output.")
            sys.exit(1)

        fmt = fmt.lower()
        try:
            if fmt == "csv":
                df.to_csv(path, index=False)
            elif fmt == "parquet":
                df.to_parquet(path, index=False)
            elif fmt == "json":
                df.to_json(path, orient="records")
            elif fmt == "excel":
                df.to_excel(path, index=False)
            else:
                cli_log.error(f"Unsupported output format: {fmt}")
                sys.exit(1)
            cli_log.info(f"Output written to {path} as {fmt.upper()}")
        except Exception as e:
            cli_log.error(f"Failed to write output: {e}")
            sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if "--debug" in sys.argv:  # optional
            traceback.print_exc()
        sys.exit(1)
