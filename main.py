#!/usr/bin/env python3
"""
Main entry point for the MicroStrategy Unified CLI tool.
"""

import argparse
import sys

from mstr_cli.core.auth import MstrClient
from mstr_cli.core.config import load_environment
from mstr_cli.commands import model as model_commands
from mstr_cli.commands import dossier as dossier_commands, dictionary as dictionary_commands


def main():
    """
    Parses command-line arguments and dispatches to the appropriate command function.
    """
    # Main parser
    parser = argparse.ArgumentParser(
        description="A unified command-line tool for MicroStrategy operations."
    )
    subparsers = parser.add_subparsers(dest="command_group", required=True)

    # === Model Command Group ===
    model_parser = subparsers.add_parser("model", help="Commands for managing Mosaic models")
    model_subparsers = model_parser.add_subparsers(dest="command", required=True)

    # --- model create ---
    create_model_parser = model_subparsers.add_parser(
        "create", help="Create a new Mosaic data model"
    )
    create_model_parser.add_argument("--model-name", required=True, help="Name for the new Mosaic model")
    create_model_parser.add_argument("--source-config", help="JSON file with list of source table definitions")
    create_model_parser.add_argument("--datasource", help="Datasource name (for single-source mode)")
    create_model_parser.add_argument("--table", help="Table name (for single-source mode)")
    create_model_parser.add_argument("--schema", default="public", help="Schema name (for single-source mode)")
    create_model_parser.add_argument("--date-columns", default="", help="Comma-separated columns to convert to date (single-source mode)")
    create_model_parser.add_argument("--description", default="", help="Model description")
    create_model_parser.add_argument("--folder-id", default=None, help="Destination folder ID for the model")
    create_model_parser.add_argument("--keep-workspace", action="store_true", help="Do not delete the workspace after committing")
    create_model_parser.set_defaults(func=model_commands.create_model)

    # --- model add-column ---
    add_column_parser = model_subparsers.add_parser("add-column", help="Add a new column and attribute to a model table")
    add_column_parser.add_argument("--model-id", required=True, help="Mosaic model object ID")
    add_column_parser.add_argument("--table-name", required=True, help="Logical table name in the model")
    add_column_parser.add_argument("--column-name", required=True, help="Warehouse column name to add")
    add_column_parser.add_argument("--attr-name", help="Display name for the new attribute (defaults to title-cased column name)")
    add_column_parser.add_argument("--description", default="", help="Object description")
    add_column_parser.add_argument("--infer-type", action="store_true", default=True, help="Use AI service to infer data type (default)")
    add_column_parser.set_defaults(func=model_commands.add_column)

    # --- model create-metric ---
    create_metric_parser = model_subparsers.add_parser("create-metric", help="Create a calculated metric in a model")
    create_metric_parser.add_argument("--model-id", required=True, help="Mosaic model object ID")
    create_metric_parser.add_argument("--metric-name", required=True, help="Name for the new calculated metric")
    create_metric_parser.add_argument("--source", required=True, help="Source for the metric. Sum mode: 'MetricName'. Calc mode: 'Metric1-Metric2'")
    create_metric_parser.add_argument("--mode", choices=["sum", "calc"], default="calc", help="Metric creation mode")
    create_metric_parser.add_argument("--skip-if-exists", action="store_true", help="Skip creation if a metric with the same name already exists")
    create_metric_parser.set_defaults(func=model_commands.create_metric)

    # --- model delete-attribute ---
    delete_attr_parser = model_subparsers.add_parser("delete-attribute", help="Delete an attribute from a model")
    delete_attr_parser.add_argument("--model-id", required=True, help="Mosaic model object ID")
    delete_attr_parser.add_argument("--attr-name", required=True, help="Exact name of the attribute to delete")
    delete_attr_parser.set_defaults(func=model_commands.delete_attribute)

    # --- model delete-metric ---
    delete_metric_parser = model_subparsers.add_parser("delete-metric", help="Delete a metric from a model")
    delete_metric_parser.add_argument("--model-id", required=True, help="Mosaic model object ID")
    delete_metric_parser.add_argument("--metric-name", required=True, help="Exact name of the metric to delete")
    delete_metric_parser.set_defaults(func=model_commands.delete_metric)

    # === Dossier Command Group ===
    dossier_parser = subparsers.add_parser("dossier", help="Commands for managing Dossiers")
    dossier_subparsers = dossier_parser.add_subparsers(dest="command", required=True)
    edit_dossier_parser = dossier_subparsers.add_parser("edit", help="Edit a visualization in a dossier by replacing a metric")
    edit_dossier_parser.add_argument("--dossier-id", required=True, help="ID of the dossier to edit")
    edit_dossier_parser.add_argument("--metric-to-remove", required=True, help="Name of the metric to replace")
    edit_dossier_parser.add_argument("--metric-to-add", required=True, help="Name of the metric to add")
    edit_dossier_parser.set_defaults(func=dossier_commands.edit_dossier)

    # === Dictionary Command Group ===
    dict_parser = subparsers.add_parser("dictionary", help="Commands for data dictionary generation")
    dict_subparsers = dict_parser.add_subparsers(dest="command", required=True)
    generate_dict_parser = dict_subparsers.add_parser("generate", help="Compare a model against its physical data sources")
    generate_dict_parser.add_argument("--model-id", required=True, help="Mosaic model ID")
    generate_dict_parser.add_argument("--datasource", action="append", help="Datasource display name (paired with --table)")
    generate_dict_parser.add_argument("--table", action="append", help="Table name (paired with --datasource)")
    generate_dict_parser.add_argument("--schema", action="append", help="Schema name (default: public)")
    generate_dict_parser.add_argument("--output", help="Output CSV path (default: data_dictionary_<model_id>.csv)")
    generate_dict_parser.set_defaults(func=dictionary_commands.generate_dictionary)

    # Global arguments
    parser.add_argument("--project", help="MSTR project name (overrides .env)")

    args = parser.parse_args()

    # Validate arguments for model create
    if args.command_group == "model" and args.command == "create":
        if args.source_config:
            if args.datasource or args.table:
                print("[!] Warning: --source-config provided; ignoring --datasource / --table")
        else:
            if not args.datasource or not args.table:
                create_model_parser.error("Either --source-config or both --datasource and --table are required")

    # --- Execution ---
    client = None
    try:
        env = load_environment()
        client = MstrClient(
            base_url=env["MSTR_BASE_URL"],
            username=env["MSTR_USERNAME"],
            password=env["MSTR_PASSWORD"],
            project_name=args.project or env.get("MSTR_PROJECT_NAME", "Shared Studio"),
            login_mode=int(env.get("MSTR_LOGIN_MODE", "1")),
        )
        client.model_id = getattr(args, 'model_id', None) # Make model_id available to utils

        # Call the function associated with the chosen command
        args.func(client, args)

    except FileNotFoundError as e:
        print(f"[ERROR] Configuration error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    main()