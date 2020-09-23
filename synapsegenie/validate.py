#!/usr/bin/env python3
import importlib
import inspect
import logging
import sys

import synapseclient
from synapseclient.core.exceptions import SynapseHTTPError

from . import example_filetype_format, process_functions

logger = logging.getLogger(__name__)


class ValidationHelper(object):

    # Used for the kwargs in validate_single_file
    # Overload this per class
    _validate_kwargs = []

    def __init__(self, syn, project_id, center, entitylist,
                 format_registry=None, file_type=None):
        """A validator helper class for a center's files.

        Args:
            syn: a synapseclient.Synapse object
            project_id: Synapse Project ID where files are stored and configured.
            center: The participating center name.
            filepathlist: a list of file paths.
            format_registry: A dictionary mapping file format name to the
                             format class.
            file_type: Specify file type to skip filename validation
        """
        self._synapse_client = syn
        self._project = syn.get(project_id)
        self.entitylist = entitylist
        self.center = center
        self._format_registry = format_registry
        self.file_type = (self.determine_filetype()
                          if file_type is None else file_type)

    def determine_filetype(self):
        """Gets the file type of the file by validating its filename

        Args:
            syn: Synapse object
            filepathlist: list of filepaths to center files

        Returns:
            str: File type of input files.  None if no filetype found

        """
        filetype = None
        # Loop through file formats
        for file_format in self._format_registry:
            validator = self._format_registry[file_format](self._synapse_client, self.center)
            try:
                filenames = [entity.name for entity in self.entitylist]
                filetype = validator.validateFilename(filenames)
            except AssertionError:
                continue
            # If valid filename, return file type.
            if filetype is not None:
                break
        return filetype

    def validate_single_file(self, **kwargs):
        """Validate a submitted file unit.

        Returns:
            message: errors and warnings
            valid: Boolean value of validation status
        """

        if self.file_type not in self._format_registry:
            valid = False
            errors = "Your filename is incorrect! Please change your filename before you run the validator or specify --filetype if you are running the validator locally"
            warnings = ""
        else:
            mykwargs = {}
            for required_parameter in self._validate_kwargs:
                assert required_parameter in kwargs.keys(), \
                    "%s not in parameter list" % required_parameter
                mykwargs[required_parameter] = kwargs[required_parameter]
                mykwargs['project_id'] = self._project.id

            validator_cls = self._format_registry[self.file_type]
            validator = validator_cls(self._synapse_client, self.center)
            filepathlist = [entity.path for entity in self.entitylist]
            valid, errors, warnings = validator.validate(filePathList=filepathlist,
                                                         **mykwargs)

        # Complete error message
        message = collect_errors_and_warnings(errors, warnings)

        return (valid, message)


def collect_errors_and_warnings(errors, warnings):
    '''Aggregates error and warnings into a string.

    Args:
        errors: string of file errors, separated by new lines.
        warnings: string of file warnings, separated by new lines.

    Returns:
        message - errors + warnings
    '''
    # Complete error message
    message = "----------------ERRORS----------------\n"
    if errors == "":
        message = "YOUR FILE IS VALIDATED!\n"
        logger.info(message)
    else:
        for error in errors.split("\n"):
            if error != '':
                logger.error(error)
        message += errors
    if warnings != "":
        for warning in warnings.split("\n"):
            if warning != '':
                logger.warning(warning)
        message += "-------------WARNINGS-------------\n" + warnings
    return message


def get_config(syn, synid):
    """Gets Synapse database to Table mapping in dict

    Args:
        syn: Synapse connection
        synid: Synapse id of database mapping table

    Returns:
        dict: {'databasename': 'synid'}

    """
    config = syn.tableQuery('SELECT * FROM {}'.format(synid))
    configdf = config.asDataFrame()
    configdf.index = configdf['Database']
    config_dict = configdf.to_dict()
    return config_dict['Id']


def _check_parentid_permission_container(syn, parentid):
    """Checks permission / container
    # TODO: Currently only checks if a user has READ permissions
    """
    if parentid is not None:
        try:
            syn_ent = syn.get(parentid, downloadFile=False)
            # If not container, throw an assertion
            assert synapseclient.entity.is_container(syn_ent)
        except (SynapseHTTPError, AssertionError):
            raise ValueError(
                "Provided Synapse id must be your input folder Synapse id "
                "or a Synapse Id of a folder inside your input directory")


def _check_center_input(center, center_list):
    """Checks center input

    Args:
        center: Center name
        center_list: List of allowed centers

    Raises:
        ValueError: If specify a center not part of the center list

    """
    if center not in center_list:
        raise ValueError("Must specify one of these "
                         f"centers: {', '.join(center_list)}")


def _upload_to_synapse(syn, filepaths, valid, parentid=None):
    """
    Upload to synapse if parentid is specified and valid

    Args:
        syn: Synapse object
        filepaths: List of file paths
        valid: Boolean value for validity of file
        parentid: Synapse id of container. Default is None

    """
    if parentid is not None and valid:
        logger.info("Uploading file to {}".format(parentid))
        for path in filepaths:
            file_ent = synapseclient.File(path, parent=parentid)
            ent = syn.store(file_ent)
            logger.info("Stored to {}".format(ent.id))
