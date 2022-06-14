from collections import defaultdict
import datetime
import logging
import os
import shutil
import tempfile
from typing import Dict

import synapseclient
from synapseclient import Synapse
from synapseclient.core.utils import to_unix_epoch_time
import synapseutils
import pandas as pd

from . import process_functions, input_to_database
from .example_filetype_format import FileTypeFormat
from .validate import ValidationHelper

logger = logging.getLogger(__name__)


# def _process_tracker():
    # processTrackerSynId = process_functions.get_database_synid(
    #     syn, "processTracker",
    #     database_mappingdf=database_to_synid_mappingdf)
    # # Add process tracker for time start
    # processTracker = syn.tableQuery(
    #     "SELECT timeStartProcessing FROM {} "
    #     "where center = '{}' and "
    #     "processingType = '{}'".format(
    #         processTrackerSynId, center, process))
    # processTrackerDf = processTracker.asDataFrame()
    # if len(processTrackerDf) == 0:
    #     new_rows = [[
    #         center,
    #         str(int(time.time()*1000)),
    #         str(int(time.time()*1000)),
    #         process]]

    #     syn.store(synapseclient.Table(
    #         processTrackerSynId, new_rows))
    # else:
    #     processTrackerDf['timeStartProcessing'][0] = \
    #         str(int(time.time()*1000))
    #     syn.store(synapseclient.Table(
    #         processTrackerSynId, processTrackerDf))

    # Should add in this process end tracking
    # before the deletion of samples
    # processTracker = syn.tableQuery(
    #     "SELECT timeEndProcessing FROM {synid} where center = '{center}' "
    #     "and processingType = '{processtype}'".format(
    #         synid=processTrackerSynId,
    #         center=center,
    #         processtype=process))
    # processTrackerDf = processTracker.asDataFrame()
    # processTrackerDf['timeEndProcessing'][0] = str(int(time.time()*1000))
    # syn.store(synapseclient.Table(processTrackerSynId, processTrackerDf))

    # logger.info("SAMPLE/PATIENT RETRACTION")
    # toRetract.retract(syn, project_id=project_id)


class InputToDatabase:
    """Validate and process Synapse Files scoped by a Table or Fileview and
    store them into Synapse Tables"""
    def __init__(self, syn: Synapse, project_id: str, db_configuration: dict,
                 format_registry: Dict[str, FileTypeFormat],
                 validator_cls: ValidationHelper):
        """
        Args:
            syn: Synapse connection
            project_id: The Id of a Synapse Project
            db_configuration: The mapping between Synapse Id and
                              Synapse resources
            format_registry: A mapping of functions that extend FileTypeFormat
            validator_cls: A class that extends ValidationHelper
        """
        self.syn = syn
        self.project_id = project_id
        self.db_configuration = db_configuration
        self.format_registry = format_registry
        self.validator_cls = validator_cls

    def get_input_files(self, synid: str) -> list:
        """Downloads input files from fileview or specified Synapse id

        Args:
            syn: Synapse connection
            synid: Synapse Folder or Project

        Returns:
            List of Synapse File Entities

        """
        logger.info("GETTING INPUT FILES")
        file_list = []
        fileview_id = self.db_configuration.get("fileview")
        if fileview_id is None:
            scope_ids = [synid]
        else:
            fileview_ent = self.syn.get(fileview_id)
            scope_ids = [f"syn{scope_id}"
                         for scope_id in fileview_ent.scopeIds]

        for scope_id in scope_ids:
            entities = synapseutils.syncFromSynapse(self.syn, scope_id)
            file_list.extend(entities)

        return file_list

    def workflow(self, only_validate, input_folder_mapping,
                 groupby='center', groupby_value=None):
        """Validate and process each center's input files"""

        # path_to_genie = os.path.realpath(os.path.join(
        #    process_functions.SCRIPT_DIR, "../"))
        # Make the synapsecache dir the genie input folder for now
        # The main reason for this is because the .synaspecache dir
        # is mounted by batch
        # TODO: Specify workdir parameter
        path_to_genie = os.path.expanduser("~/.synapseCache")
        workdir = tempfile.mkdtemp(dir=path_to_genie,
                                   prefix=f'{groupby_value}-')
        # print(workdir.name)
        # Set log handler
        if only_validate:
            log_path = os.path.join(workdir,
                                    f"{groupby_value}_validation_log.txt")
        else:
            log_path = os.path.join(workdir, f"{groupby_value}_log.txt")

        log_formatter = logging.Formatter(
            "%(asctime)s [%(name)s][%(levelname)s] %(message)s"
        )
        file_handler = logging.FileHandler(log_path, mode='w')
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

        if groupby_value is not None:
            input_folder_synid = input_folder_mapping['inputSynId'][
                groupby_value
            ]
        # center_input_synid = center_mapping_df['inputSynId'][
        #     center_mapping_df[groupby] == groupby_value][0]
        logger.info(f"Processing: {groupby_value}")
        input_files = self.get_input_files(input_folder_synid)

        # only validate if there are center files
        if input_files:
            validfiles = self.validation(groupby_value, input_files)
        else:
            logger.info(f"{groupby_value} has no input files")
            return

        # validfiles is a dataframe
        if len(validfiles.index) and not only_validate:
            self.processfiles(
                validfiles=validfiles,
                center=groupby_value, workdir=workdir,
                center_mapping_df=input_folder_mapping
            )


        # else:
        #     message_out = \
        #         "{} does not have any valid files" if not only_validate \
        #         else "ONLY VALIDATION OCCURED FOR {}"
        #     logger.info(message_out.format(groupby_value))

        # logger.info("Processing Done")

        # # Store log file
        # log_folder_synid = process_functions.get_database_synid(
        #     syn, "logs", database_mappingdf=database_to_synid_mappingdf
        # )
        # syn.store(synapseclient.File(log_path, parentId=log_folder_synid))
        # shutil.rmtree(workdir, ignore_errors=True)
        logger.info("ALL PROCESSES COMPLETE")

    # TODO: Create ProcessHelper class
    def processfiles(self, validfiles, center, workdir,
                     center_mapping_df):
        """Processing validated files

        Args:
            syn: Synapse object
            validfiles: pandas dataframe containing validated files
                        has 'id', 'path', and 'fileType' column
            center: GENIE center name
            workdir: Path to workdir
            center_mapping_df: Center mapping dataframe
            databaseToSynIdMappingDf: Database to synapse id mapping dataframe

        """
        logger.info(f"PROCESSING {center} FILES: {len(validfiles)}")
        # center_staging_synid = center_mapping_df.query(
        #     f"center == '{center}'").stagingSynId.iloc[0]
        # TODO: Remove or add in staging folder
        center_staging_synid = None

        for _, row in validfiles.iterrows():
            filetype = row['fileType']
            # filename = os.path.basename(filePath)
            newpath = os.path.join(workdir, row['name'])
            # store = True
            tableid = self.db_configuration.get(filetype)
            # tableid is a series, so much check actual length
            # Can't do `if tableid:`
            # if len(tableid) == 0:
            #     tableid = None
            # else:
            #     tableid = tableid[0]
            if filetype is not None:
                processor = self.format_registry[filetype](self.syn, center)
                processor.process(
                    filePath=row['path'], newPath=newpath,
                    parentId=center_staging_synid, databaseSynId=tableid,
                    fileSynId=row['id'],
                    databaseToSynIdMappingDf=self.db_configuration
                )

        logger.info("ALL DATA STORED IN DATABASE")

    def validation(self, center, center_files, groupby='center'):
        '''
        Validation of all center files

        Args:
            syn: Synapse object
            center: Center name
            center_mapping_df: center mapping dataframe

        Returns:
            dataframe: Valid files
        '''
        logger.info(f"{center} has uploaded {len(center_files)} files.")
        validation_status_synid = self.db_configuration['validationStatus']
        error_tracker_synid = self.db_configuration['errorTracker']

        # Make sure the vcf validation statuses don't get wiped away
        # If process is not vcf, the vcf files are not downloaded
        # TODO: Add parameter to exclude types
        exclude_type = ''
        # id, md5, status, name, center, modifiedOn, fileType
        validation_status_table = self.syn.tableQuery(
            f"SELECT * FROM {validation_status_synid} where "
            f"{groupby} = '{center}' and fileType <> '{exclude_type}'"
        )
        # id, center, errors, name, fileType
        error_tracker_table = self.syn.tableQuery(
            f"SELECT * FROM {error_tracker_synid} where "
            f"{groupby} = '{center}' and fileType <> '{exclude_type}'"
        )

        input_valid_statuses = []
        invalid_errors = []

        # This default dict will capture all the error messages to send to
        # particular users
        user_message_dict = defaultdict(list)

        for entity in center_files:
            status, errors, messages_to_send = self.validatefile(
                entity, validation_status_table,
                error_tracker_table,
                center=center
            )

            input_valid_statuses.extend(status)
            if errors is not None:
                invalid_errors.extend(errors)

            if messages_to_send:
                logger.debug("Collating messages to send to users.")
                for filenames, messages, users in messages_to_send:
                    file_messages = dict(filenames=filenames,
                                         messages=messages)
                    # Must get unique set of users or there
                    # will be duplicated error messages sent in the email
                    for user in set(users):
                        user_message_dict[user].append(file_messages)

        validation_statusdf = input_to_database.build_validation_status_table(
            input_valid_statuses
        )
        error_trackingdf = input_to_database.build_error_tracking_table(
            invalid_errors
        )

        new_tables = input_to_database._update_tables_content(
            validation_statusdf, error_trackingdf
        )
        validation_statusdf = new_tables['validation_statusdf']
        error_trackingdf = new_tables['error_trackingdf']
        duplicated_filesdf = new_tables['duplicated_filesdf']

        # In GENIE, we not only want to send out file format errors, but
        # also when there are duplicated errors.  The function below will
        # append duplication errors as an email to send to users (if applicable)
        user_message_dict = input_to_database.append_duplication_errors(
            duplicated_filesdf, user_message_dict
        )

        for user, message_objs in user_message_dict.items():
            logger.debug(f"Sending messages to user {user}.")

            input_to_database._send_validation_error_email(
                syn=self.syn, user=user, message_objs=message_objs
            )

        input_to_database.update_status_and_error_tables(
            syn=self.syn,
            input_valid_statusdf=validation_statusdf,
            invalid_errorsdf=error_trackingdf,
            validation_status_table=validation_status_table,
            error_tracker_table=error_tracker_table
        )

        valid_filesdf = validation_statusdf.query('status == "VALIDATED"')
        return valid_filesdf[['id', 'path', 'fileType', 'name']]

    def validatefile(self, entity, validation_status_table,
                     error_tracker_table, center):
        '''Validate a list of entities.

        If a file has not changed, then it doesn't need to be validated.

        Args:
            syn: Synapse object
            entities: A list of entities for a single file 'type'
                    (usually a single file, but clinical can have two)
            validation_statusdf: Validation status dataframe
            error_trackerdf: Invalid files error tracking dataframe
            center: Center of interest

        Returns:
            tuple: input_status_list - status of input files,
                invalid_errors_list - error list
                messages_to_send - list of tuples with (filenames, message, file_users)

        '''
        # Legacy code that only accepts lists for entities
        # TODO: make is so that code everywhere takes single entity
        entities = [entity]
        filenames = [entity.name for entity in entities]

        logger.info(f"VALIDATING {entity.name}")

        file_users = [entity.modifiedBy, entity.createdBy]

        check_file_status = input_to_database.check_existing_file_status(
            validation_status_table, error_tracker_table, entities
        )

        status_list = check_file_status['status_list']
        error_list = check_file_status['error_list']

        messages_to_send = []

        # Need to figure out to how to remove this
        # This must pass in filenames, because filetype is determined by entity
        # name Not by actual path of file
        # filetype can also be set on the entity itself which would then avoid
        # having determine filetype called
        validator = self.validator_cls(
            syn=self.syn, project_id=self.project_id,
            center=center,
            entitylist=entities,
            format_registry=self.format_registry,
            file_type=entity.annotations.get('filetype')
        )
        filetype = validator.file_type
        if check_file_status['to_validate']:
            valid, message = validator.validate_single_file()
            logger.info("VALIDATION COMPLETE")
            status_tuple = input_to_database._get_status_and_error_list(
                valid, message, entities
            )
            input_status_list, invalid_errors_list = status_tuple
            # Send email the first time the file is invalid
            if invalid_errors_list:
                messages_to_send.append((filenames, message, file_users))
        else:
            input_status_list = [
                {'entity': entity, 'status': status}
                for entity, status in zip(entities, status_list)
            ]
            invalid_errors_list = [
                {'entity': entity, 'errors': errors}
                for entity, errors in zip(entities, error_list)
            ]
        # add in static filetype and center information
        for input_status in input_status_list:
            input_status.update({'fileType': filetype, 'center': center})
        # An empty list is returned if there are no errors,
        # so nothing will be appended
        for invalid_errors in invalid_errors_list:
            invalid_errors.update({'fileType': filetype, 'center': center})
        return input_status_list, invalid_errors_list, messages_to_send
