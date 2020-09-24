#!/usr/bin/env python3
# noqa pylint: disable=line-too-long
"""synapsegenie cli"""
import argparse
from datetime import date
import logging

import synapseclient

from synapsegenie import (bootstrap, config, input_to_database,
                          process_functions, validate, write_invalid_reasons)

from .__version__ import __version__


logger = logging.getLogger(__name__)


def synapse_login(username=None, password=None):
    """
    This function logs into synapse for you if credentials are saved.
    If not saved, then user is prompted username and password.

    :returns:     Synapseclient object
    """
    try:
        syn = synapseclient.login(silent=True)
    except Exception:
        if username is None and password is None:
            raise ValueError("Please specify --syn_user, --syn_pass to specify your Synapse "
                             "login. Please view https://docs.synapse.org/articles/client_configuration.html"
                             "to learn about logging into Synapse via the Python client.")
        syn = synapseclient.login(email=username, password=password,
                                  silent=True)
    return syn


def bootstrap_infra(syn, args):
    """Create GENIE-like infrastructure"""
    bootstrap.main(syn)


def validate_single_file_cli_wrapper(syn, args):
    """This is the main entry point to the genie command line tool."""
    # Check parentid argparse
    validate._check_parentid_permission_container(syn, args.parentid)

    databasetosynid_mappingdf = process_functions.get_synid_database_mappingdf(
        syn, project_id=args.project_id)

    synid = databasetosynid_mappingdf.query('Database == "centerMapping"').Id

    center_mapping = syn.tableQuery('select * from {}'.format(synid.iloc[0]))
    center_mapping_df = center_mapping.asDataFrame()

    # Check center argparse
    validate._check_center_input(args.center, center_mapping_df.center.tolist())

    validator_cls = config.collect_validation_helper(
        args.format_registry_packages
    )

    format_registry = config.collect_format_types(
        args.format_registry_packages
    )
    logger.debug(f"Using {format_registry} file formats.")
    entity_list = [synapseclient.File(name=filepath, path=filepath,
                                      parentId=None)
                   for filepath in args.filepath]

    validator = validator_cls(syn=syn, project_id=args.project_id,
                              center=args.center,
                              entitylist=entity_list,
                              format_registry=format_registry,
                              file_type=args.filetype)
    mykwargs = dict(project_id=args.project_id)
    valid, message = validator.validate_single_file(**mykwargs)

    # Upload to synapse if parentid is specified and valid
    validate._upload_to_synapse(syn, args.filepath, valid, parentid=args.parentid)


def process_cli_wrapper(syn, args):
    """Process CLI wrapper"""
    process(syn, args.project_id, center=args.center,
            pemfile=args.pemfile, delete_old=args.delete_old,
            only_validate=args.only_validate, debug=args.debug,
            format_registry_packages=args.format_registry_packages)


def process(syn, project_id, center=None, pemfile=None,
            delete_old=False, only_validate=False, debug=False,
            format_registry_packages=None):
    """Process files"""
    # Get the Synapse Project where data is stored
    # Should have annotations to find the table lookup
    db_mapping_info = process_functions.get_dbmapping(syn, project_id)
    database_mappingdf = db_mapping_info['df']

    center_mapping_id = process_functions.getDatabaseSynId(
        syn, "centerMapping",
        databaseToSynIdMappingDf=database_mappingdf
    )

    center_mapping = syn.tableQuery(f'SELECT * FROM {center_mapping_id}')
    center_mapping_df = center_mapping.asDataFrame()

    if center is not None:
        assert center in center_mapping_df.center.tolist(), (
            "Must specify one of these centers: {}".format(
                ", ".join(center_mapping_df.center)))
        centers = [center]
    else:
        center_mapping_df = center_mapping_df[
            ~center_mapping_df['inputSynId'].isnull()
        ]
        # release is a bool column
        center_mapping_df = center_mapping_df[center_mapping_df['release']]
        centers = center_mapping_df.center

    validator_cls = config.collect_validation_helper(format_registry_packages)

    format_registry = config.collect_format_types(format_registry_packages)

    for process_center in centers:
        input_to_database.center_input_to_database(
            syn, project_id, process_center,
            only_validate, database_mappingdf,
            center_mapping_df,
            delete_old=delete_old,
            format_registry=format_registry,
            validator_cls=validator_cls
        )

    error_tracker_synid = process_functions.getDatabaseSynId(
        syn, "errorTracker", databaseToSynIdMappingDf=database_mappingdf
    )
    # Only write out invalid reasons if the center
    # isnt specified and if only validate
    if center is None and only_validate:
        logger.info("WRITING INVALID REASONS TO CENTER STAGING DIRS")
        write_invalid_reasons.write_invalid_reasons(
            syn, center_mapping_df, error_tracker_synid
        )


def replace_db_cli_wrapper(syn, args):
    """Replace existing db with new empty db"""
    db_mapping_info = process_functions.get_dbmapping(syn, args.project_id)
    database_mappingdf = db_mapping_info['df']
    if args.filetype not in database_mappingdf['Database'].tolist():
        raise ValueError("Must specify existing database type")
    today = date.today()
    table_name = f'{args.table_name} - {today}'
    new_tables = process_functions.create_new_fileformat_table(
        syn, args.filetype, table_name, args.project_id,
        args.archive_projectid
    )
    print(new_tables['newdb_ent'])


def build_parser():
    """Build CLI parsers"""
    parser = argparse.ArgumentParser(
        description='synapsegenie will validate and process files in a '
                    'specified project given a file format registry package.'
    )

    parser.add_argument("--syn_user", type=str, help='Synapse username')

    parser.add_argument("--syn_pass", type=str, help='Synapse password')

    parser.add_argument('-v', '--version', action='version',
                        version='%(prog)s {}'.format(__version__))

    # Create parent parsers that contain arguments per command but don't
    # want them to be on a top level parser.
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--format_registry_packages", type=str, nargs="+",
        default=["example_registry"],
        help="Python package name(s) to get valid file formats from "
             "(default: %(default)s)."
    )

    parent_parser.add_argument(
        "--project_id", type=str, required=True,
        help='Synapse Project ID where data is stored.'
    )

    subparsers = parser.add_subparsers(
        title='commands', description='The following commands are available:',
        help='For additional help: "synapsegenie <COMMAND> -h"'
    )

    parser_validate = subparsers.add_parser(
        'validate-single-file',
        help='Validates a file whose file format is specified by the format '
             'registry',
        parents=[parent_parser]
    )

    parser_validate.add_argument("filepath", type=str, nargs="+",
                                 help='File that you are validating.')

    parser_validate.add_argument("center", type=str, help='Center name')

    validate_group = parser_validate.add_mutually_exclusive_group()

    validate_group.add_argument(
        "--filetype", type=str,
        help='By default, the validator uses the filename to match '
             'the file format.  If your filename is incorrectly named, '
             'it will be invalid.  If you know the file format you are '
             'validating, you can ignore the filename validation and skip '
             'to file content validation.'
    )

    validate_group.add_argument(
        "--parentid", type=str, default=None,
        help='Synapse id of center input folder. '
             'If specified, your valid files will be uploaded '
             'to this directory.'
    )

    parser_validate.set_defaults(func=validate_single_file_cli_wrapper)

    parser_bootstrap = subparsers.add_parser('bootstrap-infra',
                                             help='Create GENIE-like infra',
                                             parents=[parent_parser])
    parser_bootstrap.set_defaults(func=bootstrap_infra)

    parser_process = subparsers.add_parser('process', help='Process files',
                                           parents=[parent_parser])
    parser_process.add_argument('--center', help='The centers')
    parser_process.add_argument(
        "--pemfile", type=str,
        help="Path to PEM file (genie.pem)"
    )
    parser_process.add_argument(
        "--delete_old", action='store_true',
        help="Delete all old processed and temp files"
    )
    parser_process.add_argument(
        "--only_validate", action='store_true',
        help="Only validate the files, don't process"
    )
    parser_process.add_argument(
        "--debug", action='store_true',
        help="Add debug mode to synapse"
    )
    parser_process.set_defaults(func=process_cli_wrapper)

    parser_replace_db = subparsers.add_parser(
        'replace-db',
        help='Replace existing database with new empty database',
        parents=[parent_parser]
    )
    parser_replace_db.add_argument('filetype',
                                   help='Database type to replace')
    parser_replace_db.add_argument(
        'archive_projectid',
        help='Synapse id of project to archive table'
    )
    parser_replace_db.add_argument(
        'table_name',
        help='New table name.  Will have todays date appened to it.'
    )
    parser_replace_db.set_defaults(func=replace_db_cli_wrapper)

    return parser


def main():
    """Invoke"""
    args = build_parser().parse_args()
    syn = synapse_login(args.syn_user, args.syn_pass)
    # func has to match the set_defaults
    args.func(syn, args)


if __name__ == "__main__":
    main()
