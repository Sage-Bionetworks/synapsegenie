#!/usr/bin/env python3
# noqa pylint: disable=line-too-long
"""synapsegenie cli"""
import argparse
from datetime import date
import json
import logging
import os

import synapseclient
from synapseclient import Synapse
from synapseclient.core.exceptions import (
    SynapseNoCredentialsError,
    SynapseAuthenticationError,
)

from . import (
    bootstrap,
    config,
    input_to_database,
    process_functions,
    validate,
    write_invalid_reasons,
)

from .__version__ import __version__


logger = logging.getLogger(__name__)


def synapse_login(synapse_config: str = None) -> Synapse:
    """Login to Synapse.
    1. Looks first for scheduled job secrets.
    2. Use synapse config file if provided
    3. If config not providede, Client will look for SYNAPSE_AUTH_TOKEN
       env variable

    Args:
        synapse_config: Path to synapse configuration file.

    Returns:
        Synapse connection
    """
    try:
        syn = synapseclient.Synapse(skip_checks=True, configPath=synapse_config)
        if os.getenv("SCHEDULED_JOB_SECRETS") is not None:
            secrets = json.loads(os.getenv("SCHEDULED_JOB_SECRETS"))
            syn.login(silent=True, authToken=secrets["SYNAPSE_AUTH_TOKEN"])
        else:
            syn.login(silent=True)
    except (SynapseNoCredentialsError, SynapseAuthenticationError):
        raise ValueError(
            "Login error: please make sure you have correctly "
            "configured your client."
        )
    return syn


def get_file_errors_cli_wrapper(syn, args):
    """CLI to get invalid reasons"""
    project = syn.get(args.project_id)
    db_mapping = syn.tableQuery(f"select * from {project.dbMapping[0]}")
    db_mappingdf = db_mapping.asDataFrame()
    error_tracker_synid = db_mappingdf["Id"][
        db_mappingdf["Database"] == "errorTracker"
    ][0]
    center_errors = write_invalid_reasons.get_center_invalid_errors(
        syn, error_tracker_synid
    )
    print(center_errors[args.center])


def bootstrap_infra(syn, args):
    """Create GENIE-like infrastructure"""
    # Basic setup of the project
    if args.project_name:
        # Create the project
        project = synapseclient.Project(args.project_name)
        project = syn.store(project)
    else:
        project = syn.get(args.project_id)

    bootstrap.main(
        syn,
        project=project,
        format_registry=args.format_registry_packages,
        centers=args.centers,
    )


def validate_single_file_cli_wrapper(syn, args):
    """This is the main entry point to the genie command line tool."""
    # Check parentid argparse
    validate._check_parentid_permission_container(syn, args.parentid)

    databasetosynid_json = process_functions.get_dbmapping(
        syn, project_id=args.project_id
    )
    databasetosynid_mappingdf = databasetosynid_json["df"]

    synid = databasetosynid_mappingdf.query('Database == "centerMapping"').Id

    center_mapping = syn.tableQuery("select * from {}".format(synid.iloc[0]))
    center_mapping_df = center_mapping.asDataFrame()

    # Check center argparse
    validate._check_center_input(args.center, center_mapping_df.center.tolist())

    validator_cls = config.collect_validation_helper(args.format_registry_packages)

    format_registry = config.collect_format_types(args.format_registry_packages)
    logger.debug(f"Using {format_registry} file formats.")
    entity = synapseclient.File(name=args.filepath, path=args.filepath, parentId=None)

    validator = validator_cls(
        syn=syn,
        project_id=args.project_id,
        center=args.center,
        entity=entity,
        format_registry=format_registry,
        file_type=args.filetype,
    )
    mykwargs = dict(project_id=args.project_id)
    valid, message = validator.validate_single_file(**mykwargs)

    # Upload to synapse if parentid is specified and valid
    validate._upload_to_synapse(syn, args.filepath, valid, parentid=args.parentid)


def process_cli_wrapper(syn, args):
    """Process CLI wrapper"""
    # Need to reverse the parameter
    download_files = not args.only_get_entity
    process(
        syn,
        args.project_id,
        center=args.center,
        pemfile=args.pemfile,
        delete_old=args.delete_old,
        only_validate=args.only_validate,
        debug=args.debug,
        format_registry_packages=args.format_registry_packages,
        download_files=download_files,
    )


def process(
    syn,
    project_id,
    center=None,
    pemfile=None,
    delete_old=False,
    only_validate=False,
    debug=False,
    format_registry_packages=None,
    download_files=True,
):
    """Process files"""
    # Get the Synapse Project where data is stored
    # Should have annotations to find the table lookup
    db_mapping_info = process_functions.get_dbmapping(syn, project_id)
    database_mappingdf = db_mapping_info["df"]

    center_mapping_id = process_functions.get_database_synid(
        syn, "centerMapping", database_mappingdf=database_mappingdf
    )

    center_mapping = syn.tableQuery(f"SELECT * FROM {center_mapping_id}")
    center_mapping_df = center_mapping.asDataFrame()

    if center is not None:
        assert (
            center in center_mapping_df.center.tolist()
        ), "Must specify one of these centers: {}".format(
            ", ".join(center_mapping_df.center)
        )
        centers = [center]
    else:
        center_mapping_df = center_mapping_df[~center_mapping_df["inputSynId"].isnull()]
        # release is a bool column
        center_mapping_df = center_mapping_df[center_mapping_df["release"]]
        centers = center_mapping_df.center

    validator_cls = config.collect_validation_helper(format_registry_packages)

    format_registry = config.collect_format_types(format_registry_packages)

    for process_center in centers:
        input_to_database.center_input_to_database(
            syn,
            project_id,
            process_center,
            only_validate,
            database_mappingdf,
            center_mapping_df,
            delete_old=delete_old,
            format_registry=format_registry,
            validator_cls=validator_cls,
            download_files=download_files,
        )

    # error_tracker_synid = process_functions.get_database_synid(
    #     syn, "errorTracker", database_mappingdf=database_mappingdf
    # )
    # Only write out invalid reasons if the center
    # isnt specified and if only validate
    # if center is None and only_validate:
    #     logger.info("WRITING INVALID REASONS TO CENTER STAGING DIRS")
    #     write_invalid_reasons.write(syn, center_mapping_df,
    #                                 error_tracker_synid)


def replace_db_cli_wrapper(syn, args):
    """Replace existing db with new empty db"""
    db_mapping_info = process_functions.get_dbmapping(syn, args.project_id)
    database_mappingdf = db_mapping_info["df"]
    if args.filetype not in database_mappingdf["Database"].tolist():
        raise ValueError("Must specify existing database type")
    today = date.today()
    table_name = f"{args.table_name} - {today}"
    new_tables = process_functions.create_new_fileformat_table(
        syn, args.filetype, table_name, args.project_id, args.archive_projectid
    )
    print(new_tables["newdb_ent"])


def build_parser():
    """Build CLI parsers"""
    parser = argparse.ArgumentParser(
        description="synapsegenie will validate and process files in a "
        "specified project given a file format registry package."
    )

    parser.add_argument("-c", "--synapse_config", type=str, help="Synapse config file")
    parser.add_argument(
        "-v", "--version", action="version", version="%(prog)s {}".format(__version__)
    )

    # Create parent parsers that contain arguments per command but don't
    # want them to be on a top level parser.
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--format_registry_packages",
        type=str,
        nargs="+",
        default=["example_registry"],
        help="Python package name(s) to get valid file formats from "
        "(default: %(default)s).",
    )

    parent_parser.add_argument(
        "--project_id",
        type=str,
        required=True,
        help="Synapse Project ID where data is stored.",
    )

    subparsers = parser.add_subparsers(
        title="commands",
        description="The following commands are available:",
        help='For additional help: "synapsegenie <COMMAND> -h"',
    )

    parser_validate = subparsers.add_parser(
        "validate-single-file",
        help="Validates a file whose file format is specified by the format "
        "registry",
        parents=[parent_parser],
    )

    parser_validate.add_argument(
        "filepath", type=str, nargs="+", help="File that you are validating."
    )

    parser_validate.add_argument("center", type=str, help="Center name")

    validate_group = parser_validate.add_mutually_exclusive_group()

    validate_group.add_argument(
        "--filetype",
        type=str,
        help="By default, the validator uses the filename to match "
        "the file format.  If your filename is incorrectly named, "
        "it will be invalid.  If you know the file format you are "
        "validating, you can ignore the filename validation and skip "
        "to file content validation.",
    )

    validate_group.add_argument(
        "--parentid",
        type=str,
        default=None,
        help="Synapse id of center input folder. "
        "If specified, your valid files will be uploaded "
        "to this directory.",
    )

    parser_validate.set_defaults(func=validate_single_file_cli_wrapper)

    parser_bootstrap = subparsers.add_parser(
        "bootstrap-infra", help="Create GENIE-like infra"
    )
    parser_bootstrap.add_argument(
        "--format_registry_packages",
        type=str,
        nargs="+",
        default=["example_registry"],
        help="Python package name(s) to get valid file formats from "
        "(default: %(default)s).",
    )

    bootstrap_group = parser_bootstrap.add_mutually_exclusive_group(required=True)

    bootstrap_group.add_argument(
        "--project_name",
        type=str,
        help="If you don't have an existing Synapse Project and would like "
        "to create one, please specify a name.",
    )

    bootstrap_group.add_argument(
        "--project_id",
        type=str,
        help="If you already have a synapsegenie compatible Synapse Project, "
        "please specify its Synapse Project ID.",
    )

    parser_bootstrap.add_argument(
        "--centers", help="The centers to create", nargs="+", required=True
    )

    parser_bootstrap.set_defaults(func=bootstrap_infra)

    parser_process = subparsers.add_parser(
        "process", help="Process files", parents=[parent_parser]
    )
    parser_process.add_argument("--center", help="The centers")
    parser_process.add_argument(
        "--delete_old",
        action="store_true",
        help="Delete all old processed and temp files",
    )
    parser_process.add_argument(
        "--only_validate",
        action="store_true",
        help="Only validate the files, don't process",
    )
    parser_process.add_argument(
        "--debug", action="store_true", help="Add debug mode to synapse"
    )
    parser_process.add_argument(
        "--only_get_entity",
        action="store_true",
        help="Do not download all the files.  Default: files are downloaded",
    )
    parser_process.set_defaults(func=process_cli_wrapper)

    parser_replace_db = subparsers.add_parser(
        "replace-db",
        help="Replace existing database with new empty database",
        parents=[parent_parser],
    )
    parser_replace_db.add_argument("filetype", help="Database type to replace")
    parser_replace_db.add_argument(
        "archive_projectid", help="Synapse id of project to archive table"
    )
    parser_replace_db.add_argument(
        "table_name", help="New table name.  Will have todays date appened to it."
    )
    parser_replace_db.set_defaults(func=replace_db_cli_wrapper)

    parser_get_invalid = subparsers.add_parser(
        "get-file-errors",
        help="Get the file invalid reasons for a specific center",
        parents=[parent_parser],
    )
    parser_get_invalid.add_argument("center", type=str, help="Contributing Centers")
    parser_get_invalid.set_defaults(func=get_file_errors_cli_wrapper)

    return parser


def main():
    """Invoke"""
    args = build_parser().parse_args()
    syn = synapse_login(args.synapse_config)
    # func has to match the set_defaults
    args.func(syn, args)


if __name__ == "__main__":
    main()
