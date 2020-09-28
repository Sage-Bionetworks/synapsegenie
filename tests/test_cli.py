"""Tests __main__.py which is the cli interface"""
from unittest import mock
from unittest.mock import Mock, patch

import pandas as pd
import synapseclient
from synapsegenie import __main__, config, process_functions, validate


syn = mock.create_autospec(synapseclient.Synapse)


class ArgParser:
    """Example argparser"""
    parentid = None
    filetype = None
    project_id = None
    center = "try"
    filepath = "path.csv"
    format_registry_packages = ["example_registry"]
    project_id = "syn1234"

    def asDataFrame(self):
        database_dict = {"Database": ["centerMapping"],
                         "Id": ["syn123"],
                         "center": ["try"]}
        databasetosynid_mappingdf = pd.DataFrame(database_dict)
        return databasetosynid_mappingdf


def test_perform_validate():
    """Make sure all functions are called"""
    arg = ArgParser()
    valid = True
    with patch.object(validate,
                      "_check_parentid_permission_container",
                      return_value=None) as patch_check_parentid,\
         patch.object(process_functions, "get_dbmapping",
                      return_value={"df": arg.asDataFrame()}) as patch_getdb,\
         patch.object(syn, "tableQuery",
                      return_value=arg) as patch_syn_tablequery,\
         patch.object(validate, "_check_center_input") as patch_check_center,\
         patch.object(
            config, "collect_validation_helper",
            return_value=validate.ValidationHelper) as patch_val_col,\
         patch.object(config, "collect_format_types") as patch_collect,\
         patch.object(validate.ValidationHelper,
                      "validate_single_file",
                      return_value=(valid, 'foo')) as patch_validate,\
         patch.object(validate, "_upload_to_synapse") as patch_syn_upload:
        __main__.validate_single_file_cli_wrapper(syn, arg)
        patch_check_parentid.assert_called_once_with(syn, arg.parentid)
        patch_getdb.assert_called_once_with(syn, project_id=arg.project_id)
        patch_syn_tablequery.assert_called_once_with('select * from syn123')
        patch_check_center.assert_called_once_with(arg.center, ["try"])
        patch_collect.assert_called_once_with(["example_registry"])
        patch_val_col.assert_called_once_with(["example_registry"])
        patch_validate.assert_called_once_with(project_id=arg.project_id)
        patch_syn_upload.assert_called_once_with(
            syn, arg.filepath, valid, parentid=arg.parentid
        )
