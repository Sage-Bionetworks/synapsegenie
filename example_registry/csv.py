import logging
import os

import pandas as pd

from synapsegenie.example_filetype_format import FileTypeFormat

logger = logging.getLogger(__name__)


class Csv(FileTypeFormat):

    _filetype = "csv"

    _process_kwargs = ["newPath", "databaseSynId"]

    def _validate_filetype(self, filePath):
        assert os.path.basename(filePath).endswith(".csv")

    def _get_data(self, entity):
        '''
        Each file is to be read in for validation and processing.
        This is not to be changed in any functions. If you don't
        download the files by default, you'll have to download the
        entity here.

        Args:
            filePathList:  A list of file paths (Max is 2 for the two
                           clinical files)

        Returns:
            df: Pandas dataframe of file
        '''
        # Downloaded entity if only entity is passed in
        # entity = self.syn.get(entity)
        data = pd.read_csv(entity.path, sep="\t", comment="#")
        return data

    def _process(self, path_or_data):
        path_or_data.columns = [col.upper() for col in path_or_data.columns]
        return path_or_data

    def process_steps(self, path_or_data, newPath, databaseSynId):
        df = self._process(path_or_data)
        # TODO: no center column in Synapse table
        # process_functions.update_data(
        #     syn=self.syn, databaseSynId=databaseSynId, newData=df,
        #     filterBy=self.center, toDelete=True
        # )
        df.to_csv(newPath, sep="\t", index=False)
        return newPath

    def _validate(self, path_or_data):
        total_error = ""
        warning = ""
        if path_or_data.empty:
            total_error += "{}: File must not be empty".format(self._filetype)
        return total_error, warning
