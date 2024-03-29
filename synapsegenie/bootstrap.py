"""Bootstrap the components of a project to be used with the GENIE framework.
"""
from typing import List
import random
import tempfile

import synapseclient
from synapseclient import Schema, Synapse
import pandas

from . import config, process_functions


def _create_table(
    syn: Synapse, name: str, col_config: List[dict], parent: str
) -> Schema:
    """Create Synapse Table

    Args:
        syn: Synapse connection
        name: Table name
        col_config: Column dict configuration
        parent: Synapse id of project

    Returns:
        Stored Synapse Table

    """
    cols = [synapseclient.Column(**col) for col in col_config]
    schema = process_functions._create_schema(
        syn, table_name=name, parentid=parent, columns=cols
    )
    return schema


def create_status_table(syn, parent):
    """Set up the table that holds the validation status of all submitted
    files.
    """
    status_table_col_defs = [
        {"name": "id", "columnType": "ENTITYID"},
        {"name": "md5", "columnType": "STRING", "maximumSize": 1000},
        {
            "name": "status",
            "columnType": "STRING",
            "maximumSize": 50,
            "facetType": "enumeration",
        },
        {"name": "name", "columnType": "STRING", "maximumSize": 1000},
        {
            "name": "center",
            "columnType": "STRING",
            "maximumSize": 20,
            "facetType": "enumeration",
        },
        {"name": "modifiedOn", "columnType": "DATE"},
        # {'name': 'versionNumber',
        #  'columnType': 'STRING',
        #  'maximumSize': 50},
        {"name": "fileType", "columnType": "STRING", "maximumSize": 50},
    ]
    return _create_table(
        syn, name="Status Table", col_config=status_table_col_defs, parent=parent
    )


def create_center_map_table(syn, parent):
    """Set up the table that maps the center abbreviation to the folder where
    their data is uploaded. This is used by the GENIE framework to find the
    files to validate for a center.
    """
    center_map_table_defs = [
        {"name": "name", "columnType": "STRING", "maximumSize": 250},
        {"name": "center", "columnType": "STRING", "maximumSize": 50},
        {"name": "inputSynId", "columnType": "ENTITYID"},
        # {'name': 'stagingSynId',
        #  'columnType': 'ENTITYID'},
        {"name": "release", "defaultValue": "false", "columnType": "BOOLEAN"},
    ]
    return _create_table(
        syn, name="Center Table", col_config=center_map_table_defs, parent=parent
    )


def create_db_mapping_table(syn, parent):
    db_map_col_defs = [
        {"name": "Database", "columnType": "STRING", "maximumSize": 50},
        {"name": "Id", "columnType": "ENTITYID"},
    ]
    return _create_table(
        syn, name="DB Mapping Table", col_config=db_map_col_defs, parent=parent
    )


def create_error_tracking_table(syn, parent):
    error_col_defs = [
        {"name": "id", "columnType": "ENTITYID"},
        {
            "name": "center",
            "columnType": "STRING",
            "maximumSize": 50,
            "facetType": "enumeration",
        },
        {"name": "errors", "columnType": "LARGETEXT"},
        {"name": "name", "columnType": "STRING", "maximumSize": 500},
        # {'name': 'versionNumber',
        #  'columnType': 'STRING',
        #  'maximumSize': 50},
        {"name": "fileType", "columnType": "STRING", "maximumSize": 50},
    ]
    return _create_table(
        syn, name="Error Table", col_config=error_col_defs, parent=parent
    )


def main(syn, project, format_registry=None, centers=None):
    # TODO: add PRIMARY_KEY annotation to each of the tables
    # Dangerous to have lists as default values
    if format_registry is None:
        format_registry = ["example_registry"]
    if centers is None:
        centers = []
    # Determine the short and long names of the centers.
    center_abbreviations = centers
    center_names = center_abbreviations

    # Create a folder for log files generated by the GENIE processes
    # of validation and updating the database tables
    logs_folder = synapseclient.Folder(name="Logs", parent=project)
    logs_folder = syn.store(logs_folder)

    # Folder for individual center folders
    root_center_folder = synapseclient.Folder(name="Centers", parent=project)
    root_center_folder = syn.store(root_center_folder)

    # The folders for each center where they will upload files for validation
    # and submission. There is one folder per center.
    # This currently deviates from the original GENIE setup of having an
    # 'Input' and 'Staging' folder for each center.
    center_folders = [
        synapseclient.Folder(name=name, parent=root_center_folder)
        for name in center_abbreviations
    ]
    center_folders = [syn.store(folder) for folder in center_folders]

    # Make some fake data that only contains basic text to check
    # for validation.

    n_files = 2  # number of files per center to create

    for folder in center_folders:
        for _ in range(n_files):
            tmp = tempfile.NamedTemporaryFile(
                prefix=f"TEST-{folder.name}", suffix=".txt"
            )
            with open(tmp.name, mode="w") as file_h:
                file_h.write(random.choice(["ERROR", "VALID", "NOPE"]))
            syn.store(synapseclient.File(tmp.name, parent=folder))

    # Set up the table that holds the validation status of all submitted
    # files.
    status_schema = create_status_table(syn, project)

    # Set up the table that maps the center abbreviation to the folder where
    # their data is uploaded. This is used by the GENIE framework to find the
    # files to validate for a center.
    center_schema = create_center_map_table(syn, project)

    # Add the center folders created above to this table.
    center_folder_ids = [folder.id for folder in center_folders]
    center_df = pandas.DataFrame(
        dict(
            name=center_names, center=center_abbreviations, inputSynId=center_folder_ids
        )
    )
    center_df["release"] = True
    existing_center = syn.tableQuery(f"select * from {center_schema.id}")
    existing_centerdf = existing_center.asDataFrame()
    process_functions.updateDatabase(
        syn, existing_centerdf, center_df, center_schema.id, ["center"], to_delete=True
    )
    # TODO: Remove centers that aren't part of the list

    # Create a table that stores the error logs for each submitted file.
    error_schema = create_error_tracking_table(syn, project)

    # Create a table that maps the various database tables to a short name.
    # This table is used in many GENIE functions to find the correct table
    # to update or get the state of something from.
    db_map_schema = create_db_mapping_table(syn, project)

    # Add dbMapping annotation
    project.annotations.dbMapping = db_map_schema.id
    project = syn.store(project)
    # Add the tables we already created to the mapping table.
    dbmap_df = pandas.DataFrame(
        dict(
            Database=[
                "centerMapping",
                "validationStatus",
                "errorTracker",
                "dbMapping",
                "logs",
            ],
            Id=[
                center_schema.id,
                status_schema.id,
                error_schema.id,
                db_map_schema.id,
                logs_folder.id,
            ],
        )
    )

    # Make a top level folder for output. Some processing for
    # file types copy a file from one place to another.
    output_folder = synapseclient.Folder(name="Output", parent=project)
    output_folder = syn.store(output_folder)

    output_folder_map = []

    # default_table_col_defs = status_table_col_defs = [
    #     {'name': 'PRIMARY_KEY',
    #      'columnType': 'STRING'}
    # ]
    # default_table_cols = [synapseclient.Column(**col)
    #                       for col in default_table_col_defs]

    default_primary_key = "PRIMARY_KEY"

    # For each file type format in the format registry, create an output
    # folder and a table.
    # Some GENIE file types copy a file to a new place, and some update a
    # table. Having both means that both of these operations will be available
    # at the beginning.
    # The mapping between the file type and the folder or table have a
    # consistent naming.
    # The key ('Database' value) is {file_type}_folder or {file_type}_table.
    # Determine which file formats are going to be used.
    format_registry = config.collect_format_types(format_registry)
    # Get existing database tables
    existing_dbmap = syn.tableQuery(f"select * from {db_map_schema.id}")
    existing_dbmapdf = existing_dbmap.asDataFrame()

    for file_type, obj in format_registry.items():
        if file_type not in existing_dbmapdf["Database"].tolist():
            file_type_folder = synapseclient.Folder(
                name=file_type, parent=output_folder
            )
            file_type_folder = syn.store(file_type_folder)
            output_folder_map.append(
                dict(Database=f"{file_type}_folder", Id=file_type_folder.id)
            )

            file_type_schema = synapseclient.Schema(name=file_type, parent=project)
            # The DCC will have to set the schema and primary key
            # after this is created.
            file_type_schema.annotations.primaryKey = default_primary_key
            file_type_schema = syn.store(file_type_schema)

            output_folder_map.append(dict(Database=file_type, Id=file_type_schema.id))
        else:
            print("Database already exists")

    # Add the folders and tables created to the mapping table.
    dbmap_df = dbmap_df.append(pandas.DataFrame(output_folder_map))

    process_functions.updateDatabase(
        syn, existing_dbmapdf, dbmap_df, db_map_schema.id, ["Database"]
    )
