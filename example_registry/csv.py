import logging
import os
from io import StringIO

from synapsegenie.example_filetype_format import FileTypeFormat
from synapsegenie import process_functions

logger = logging.getLogger(__name__)


class Csv(FileTypeFormat):

    _filetype = "csv"

    _process_kwargs = ["databaseSynId"]

    def _validate_filetype(self, filePath):
        assert os.path.basename(filePath[0]).endswith(".csv")

    def _process(self, df):
        # df.columns = [df.upper() for col in df.columns]
        df['center'] = self.center
        return df

    def process_steps(self, df, newPath, databaseSynId):
        df = self._process(df)
        process_functions.update_data(self.syn, databaseSynId, df, self.center,
                                      toDelete=True)
        df.to_csv(newPath, sep="\t", index=False)
        return newPath

    def _validate(self, df):
        total_error = StringIO()
        warning = StringIO()
        if df.empty:
            total_error.write(f"{self._filetype}: Must not be empty\n")
        if df.get("valid") is not None:
            if df['valid'][0] != "VALID":
                total_error.write(
                    f"{self._filetype}: 'valid' column must be 'VALID'\n"
                )
        else:
            total_error.write(
                f"{self._filetype}: Must have 'valid' column\n"
            )

        return total_error.getvalue(), warning.getvalue()
