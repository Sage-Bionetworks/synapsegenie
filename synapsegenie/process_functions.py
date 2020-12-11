"""Processing functions"""
from datetime import date
import logging
import os
import tempfile

import pandas as pd
import synapseclient
from synapseclient import Synapse

# Ignore SettingWithCopyWarning warning
pd.options.mode.chained_assignment = None

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def lookup_dataframe_value(df, col, query):
    '''
    Look up dataframe value given query and column

    Args:
        df: dataframe
        col: column with value to return
        query: Query for specific column

    Returns:
        value
    '''
    query = df.query(query)
    query_val = query[col].iloc[0]
    return query_val


def get_syntabledf(syn, query_string):
    '''
    Get dataframe from table query

    Args:
        syn: Synapse object
        query_string: Table query

    Returns:
        pandas dataframe with query results
    '''
    table = syn.tableQuery(query_string)
    tabledf = table.asDataFrame()
    return tabledf


def get_database_synid(syn, tablename, project_id=None,
                       database_mappingdf=None):
    '''
    Get database synapse id from database to synapse id mapping table

    Args:
        syn: Synapse object
        project_id: Synapse Project ID with a database mapping table.
        tableName: Name of synapse table
        databaseToSynIdMappingDf: Avoid calling rest call to download table
                                  if the mapping table is already downloaded

    Returns:
        str:  Synapse id of wanted database
    '''
    if database_mappingdf is None:
        database_mapping_info = get_dbmapping(syn, project_id=project_id)
        database_mappingdf = database_mapping_info['df']

    synid = lookup_dataframe_value(database_mappingdf, "Id",
                                   f'Database == "{tablename}"')
    return synid


def remove_string_float(string):
    """Pandas dataframe returns integers sometimes as floats. This function
    takes a string and removes the unnecessary .0 if the next character is
    a tab or new line.

    Args:
        string: tsv file in string format

    Return:
        string: string with float removed

    """
    string = string.replace(".0\t", "\t")
    string = string.replace(".0\n", "\n")
    return string


def remove_df_float(df, header=True):
    """Remove decimal for integers given a pandas dataframe

    Args:
        df: Pandas dataframe
        header: Should string include header row. Default to true.

    Return:
        str: tsv in text
    """
    if header:
        text = df.to_csv(sep="\t", index=False)
    else:
        text = df.to_csv(sep="\t", index=False, header=None)

    text = remove_string_float(text)
    return text


def store_file(syn, filepath, parentid, name=None, annotations={},
               used=None, executed=None):
    """Storing Files along with annotations

    Args:
        filepath: Path to file
        parentid: Project or Folder Synapse id
        name: Name of entity. Defaults to filename
        annotations:  Synapse annotations to add
        used: List of used entitys or links.
        executed:  List of scripts executed

    Returns:
        File Entity

    """
    logger.info("STORING FILES")
    file_ent = synapseclient.File(filepath, parent=parentid, name=name)
    file_ent.annotations.update(annotations)
    file_ent = syn.store(file_ent, used=used, executed=executed)
    return file_ent


def _check_valid_df(df, col):
    '''
    Checking if variable is a pandas dataframe and column specified exist

    Args:
        df: Pandas dataframe
        col: Column name
    '''
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Must pass in pandas dataframe")
    if df.get(col) is None:
        raise ValueError(f"'{col}' column must exist in dataframe")


def _get_left_diff_df(left, right, checkby):
    '''
    Subset the dataframe based on 'checkby' by taking values in the left df
    that arent in the right df

    Args:
        left: Dataframe
        right: Dataframe
        checkby: Column of values to compare

    Return:
        Dataframe: Subset of dataframe from left that don't exist in the right
    '''
    _check_valid_df(left, checkby)
    _check_valid_df(right, checkby)
    diffdf = left[~left[checkby].isin(right[checkby])]
    return diffdf


def _get_left_union_df(left, right, checkby):
    '''
    Subset the dataframe based on 'checkby' by taking the union of
    values in the left df with the right df

    Args:
        left: Dataframe
        right: Dataframe
        checkby: Column of values to compare

    Return:
        Dataframe: Subset of dataframe from left that also exist in the right
    '''
    _check_valid_df(left, checkby)
    _check_valid_df(right, checkby)
    uniondf = left[left[checkby].isin(right[checkby])]
    return uniondf


def _append_rows(new_datasetdf, databasedf, checkby):
    '''
    Compares the dataset from the database and determines which rows to
    append from the dataset

    Args:
        new_datasetdf: Input data dataframe
        databasedf: Existing data dataframe
        checkby: Column of values to compare

    Return:
        Dataframe: Dataframe of rows to append
    '''
    databasedf.fillna('', inplace=True)
    new_datasetdf.fillna('', inplace=True)

    appenddf = _get_left_diff_df(new_datasetdf, databasedf, checkby)
    if not appenddf.empty:
        logger.info("Adding Rows")
    else:
        logger.info("No new rows")
    del appenddf[checkby]
    appenddf.reset_index(drop=True, inplace=True)
    return appenddf


def _delete_rows(new_datasetdf, databasedf, checkby):
    '''
    Compares the dataset from the database and determines which rows to
    delete from the dataset

    Args:
        new_datasetdf: Input data dataframe
        databasedf: Existing data dataframe
        checkby: Column of values to compare

    Return:
        Dataframe: Dataframe of rows to delete
    '''

    databasedf.fillna('', inplace=True)
    new_datasetdf.fillna('', inplace=True)
    # If the new dataset is empty, delete everything in the database
    deletedf = _get_left_diff_df(databasedf, new_datasetdf, checkby)
    if not deletedf.empty:
        logger.info("Deleting Rows")
        delete_rowid_version = pd.DataFrame([[
            rowid.split("_")[0], rowid.split("_")[1]]
            for rowid in deletedf.index])
        delete_rowid_version.reset_index(drop=True, inplace=True)
    else:
        delete_rowid_version = pd.DataFrame()
        logger.info("No deleted rows")

    # del deletedf[checkby]
    return delete_rowid_version


def _create_update_rowsdf(updating_databasedf, updatesetdf,
                          rowids, differentrows):
    '''
    Create the update dataset dataframe

    Args:
        updating_databasedf: Update database dataframe
        updatesetdf:  Update dataset dataframe
        rowids: rowids of the database (Synapse ROW_ID, ROW_VERSION)
        differentrows: vector of booleans for rows that need to be updated
                       True for update, False for not

    Returns:
        dataframe: Update dataframe
    '''
    if sum(differentrows) > 0:
        updating_databasedf.loc[differentrows] = updatesetdf.loc[differentrows]
        toupdatedf = updating_databasedf.loc[differentrows]
        logger.info("Updating rows")
        rowid_version = pd.DataFrame([[
            rowid.split("_")[0], rowid.split("_")[1]]
            for rowid, row in zip(rowids, differentrows) if row])
        toupdatedf['ROW_ID'] = rowid_version[0].values
        toupdatedf['ROW_VERSION'] = rowid_version[1].values
        toupdatedf.reset_index(drop=True, inplace=True)
    else:
        toupdatedf = pd.DataFrame()
        logger.info("No updated rows")
    return toupdatedf


def _update_rows(new_datasetdf, databasedf, checkby):
    '''
    Compares the dataset from the database and determines which rows to
    update from the dataset

    Args:
        new_datasetdf: Input data dataframe
        databasedf: Existing data dataframe
        checkby: Column of values to compare

    Return:
        Dataframe: Dataframe of rows to update
    '''
    # initial_database = databasedf.copy()
    databasedf.fillna('', inplace=True)
    new_datasetdf.fillna('', inplace=True)
    updatesetdf = _get_left_union_df(new_datasetdf, databasedf, checkby)
    updating_databasedf = _get_left_union_df(
        databasedf, new_datasetdf, checkby)

    # If you input the exact same dataframe theres nothing to update
    # must save row version and ids for later
    rowids = updating_databasedf.index.values
    # Set index values to be 'checkby' values
    updatesetdf.index = updatesetdf[checkby]
    updating_databasedf.index = updating_databasedf[checkby]
    del updatesetdf[checkby]
    del updating_databasedf[checkby]

    # Remove duplicated index values
    updatesetdf = updatesetdf[~updatesetdf.index.duplicated()]
    # Reorder dataset index
    updatesetdf = updatesetdf.loc[updating_databasedf.index]
    # Index comparison
    differences = updatesetdf != updating_databasedf
    differentrows = differences.apply(sum, axis=1) > 0

    toupdatedf = _create_update_rowsdf(
        updating_databasedf, updatesetdf, rowids, differentrows)

    return toupdatedf


def update_data(syn, databaseSynId, newData,
                filterBy, filterByColumn="CENTER",
                col=None, toDelete=False):
    databaseEnt = syn.get(databaseSynId)
    database = syn.tableQuery(
        "SELECT * FROM {} where {} ='{}'".format(
            databaseSynId, filterByColumn, filterBy))
    database = database.asDataFrame()
    if col is not None:
        database = database[col]
    else:
        newData = newData[database.columns]
    updateDatabase(
        syn, database, newData, databaseSynId,
        databaseEnt.primaryKey, toDelete)


def updateDatabase(syn, database, new_dataset, database_synid,
                   primary_key_cols, to_delete=False):
    """
    Updates synapse tables by a row identifier with another
    dataset that has the same number and order of columns

    Args:
        syn: Synapse object
        database: The synapse table (pandas dataframe)
        new_dataset: New dataset (pandas dataframe)
        databaseSynId: Synapse Id of the database table
        uniqueKeyCols: Column(s) that make up the unique key
        toDelete: Delete rows, Defaults to False

    Returns:
        Nothing
    """
    primary_key = 'UNIQUE_KEY'
    database = database.fillna("")
    orig_database_cols = database.columns
    col_order = ['ROW_ID', 'ROW_VERSION']
    col_order.extend(orig_database_cols.tolist())
    new_dataset = new_dataset.fillna("")
    # Columns must be in the same order
    new_dataset = new_dataset[orig_database_cols]
    database[primary_key_cols] = database[primary_key_cols].applymap(str)
    database[primary_key] = database[
        primary_key_cols].apply(lambda x: ' '.join(x), axis=1)

    new_dataset[primary_key_cols] = new_dataset[primary_key_cols].applymap(str)
    new_dataset[primary_key] = new_dataset[
        primary_key_cols].apply(lambda x: ' '.join(x), axis=1)

    allupdates = pd.DataFrame(columns=col_order)
    to_append_rows = _append_rows(new_dataset, database, primary_key)
    to_update_rows = _update_rows(new_dataset, database, primary_key)
    if to_delete:
        to_delete_rows = _delete_rows(new_dataset, database, primary_key)
    else:
        to_delete_rows = pd.DataFrame()
    allupdates = allupdates.append(to_append_rows, sort=False)
    allupdates = allupdates.append(to_update_rows, sort=False)

    storedatabase = False
    update_all_file = tempfile.NamedTemporaryFile(dir=SCRIPT_DIR,
                                                  delete=False)

    with open(update_all_file.name, "w") as updatefile:
        # Must write out the headers in case there are no appends or updates
        updatefile.write(",".join(col_order) + "\n")
        if not allupdates.empty:
            # This is done because of pandas typing.
            # An integer column with one NA/blank value
            # will be cast as a double.
            updatefile.write(
                allupdates[col_order]
                .to_csv(index=False, header=None)
                .replace(".0,", ",")
                .replace(".0\n", "\n"))
            storedatabase = True
        if not to_delete_rows.empty:
            updatefile.write(
                to_delete_rows
                .to_csv(index=False, header=None)
                .replace(".0,", ",")
                .replace(".0\n", "\n"))
            storedatabase = True
    if storedatabase:
        syn.store(synapseclient.Table(database_synid, update_all_file.name))
    # Delete the update file
    os.unlink(update_all_file.name)


def _create_schema(syn, table_name, parentid, columns=None, annotations=None):
    """Creates Table Schema

    Args:
        syn: Synapse object
        table_name: Name of table
        parentid: Project synapse id
        columns: Columns of Table
        annotations: Dictionary of annotations to add

    Returns:
        Schema
    """
    schema = synapseclient.Schema(name=table_name,
                                  columns=columns,
                                  parent=parentid,
                                  annotations=annotations)
    new_schema = syn.store(schema)
    return new_schema


def _update_database_mapping(syn, database_synid_mappingdf,
                             database_mapping_synid,
                             fileformat, new_tableid):
    """Updates database to synapse id mapping table
    Args:
        syn: Synapse object
        database_synid_mappingdf: Database to synapse id mapping dataframe
        database_mapping_synid: Database to synapse id table id
        fileformat: File format updated
        new_tableid: New file format table id
    Returns:
        Updated Table object
    """
    fileformat_ind = database_synid_mappingdf['Database'] == fileformat
    # Store in the new database synid
    database_synid_mappingdf['Id'][fileformat_ind] = new_tableid
    # Only update the one row
    to_update_row = database_synid_mappingdf[fileformat_ind]

    syn.store(synapseclient.Table(database_mapping_synid, to_update_row))
    return database_synid_mappingdf


# TODO: deprecate once move function is out of the cli into the
# client master branch
def _move_entity(syn, ent, parentid, name=None):
    """Moves an entity (works like linux mv)
    Args:
        syn: Synapse object
        ent: Synapse Entity
        parentid: Synapse Project id
        name: New Entity name if a new name is desired
    Returns:
        Moved Entity
    """
    ent.parentId = parentid
    if name is not None:
        ent.name = name
    moved_ent = syn.store(ent)
    return moved_ent


def get_dbmapping(syn: Synapse, project_id: str) -> dict:
    """Gets database mapping information
    Args:
        syn: Synapse connection
        project_id: Project id where new data lives
    Returns:
        {'synid': database mapping syn id,
         'df': database mapping pd.DataFrame}
    """
    project_ent = syn.get(project_id)
    dbmapping_synid = project_ent.annotations.get("dbMapping", "")[0]
    database_mappingdf = get_syntabledf(
        syn, f'select * from {dbmapping_synid}'
    )
    return {'synid': dbmapping_synid,
            'df': database_mappingdf}


def create_new_fileformat_table(syn: Synapse,
                                file_format: str,
                                newdb_name: str,
                                projectid: str,
                                archive_projectid: str) -> dict:
    """Creates new database table based on old database table and archives
    old database table
    Args:
        syn: Synapse object
        file_format: File format to update
        newdb_name: Name of new database table
        projectid: Project id where new database should live
        archive_projectid: Project id where old database should be moved
    Returns:
        {"newdb_ent": New database synapseclient.Table,
         "newdb_mappingdf": new databse pd.DataFrame,
         "moved_ent": old database synpaseclient.Table}
    """
    db_info = get_dbmapping(syn, projectid)
    database_mappingdf = db_info['df']
    dbmapping_synid = db_info['synid']

    olddb_synid = get_database_synid(syn, file_format,
                                     database_mappingdf=database_mappingdf)
    olddb_ent = syn.get(olddb_synid)
    olddb_columns = list(syn.getTableColumns(olddb_synid))

    newdb_ent = _create_schema(syn, table_name=newdb_name,
                               columns=olddb_columns,
                               parentid=projectid,
                               annotations=olddb_ent.annotations)

    newdb_mappingdf = _update_database_mapping(syn, database_mappingdf,
                                               dbmapping_synid,
                                               file_format, newdb_ent.id)
    # Automatically rename the archived entity with ARCHIVED
    # This will attempt to resolve any issues if the table already exists at
    # location
    new_table_name = f"ARCHIVED {date.today()}-{olddb_ent.name}"
    moved_ent = _move_entity(syn, olddb_ent, archive_projectid,
                             name=new_table_name)
    return {"newdb_ent": newdb_ent,
            "newdb_mappingdf": newdb_mappingdf,
            "moved_ent": moved_ent}
