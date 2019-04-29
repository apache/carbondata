import pyarrow as pa
from pyarrow.filesystem import (_get_fs_from_path)
from py4j.java_gateway import JavaGateway
from pyarrow.parquet import ParquetFile

_carbon_application = None
def __getcarbonapplication__():
    global _carbon_application
    if _carbon_application is None:
        gateway = JavaGateway()
        _carbon_application = gateway.entry_point
        return _carbon_application
    else:
        return _carbon_application

class _CarbonReader(object):
    def __init__(self, path):
        self.path = path
    def read(self, columns):
        column_name_str = ','.join(columns)
        buf = __getcarbonapplication__().readArrowBatch(self.path, column_name_str)
        reader = pa.RecordBatchFileReader(pa.BufferReader(buf))
        data = reader.read_all()
        return data


class CarbonDataset(object):
    def __init__(self, path):
        self.path = path
        self.fs = _get_fs_from_path(path)
        self.pieces = list()
        carbon_splits = __getcarbonapplication__().getSplits(self.path)
        for split in carbon_splits:
            self.pieces.append(CarbonDatasetPiece(split))
        self.number_of_splits = len(self.pieces)
        self.schema = self.getArrowSchema()
        #TODO add mechanism to get the file path based on file filter
        self.common_metadata_path = path + '/_common_metadata'
        self.common_metadata = None
        try:
            if self.fs.exists(self.common_metadata_path):
                with self.fs.open(self.common_metadata_path) as f:
                    self.common_metadata = ParquetFile(f).metadata
        except:
            self.common_metadata = None

    def getArrowSchema(self):
        buf = __getcarbonapplication__().readSchema(self.path)
        reader = pa.RecordBatchFileReader(pa.BufferReader(buf))
        return reader.read_all().schema

class CarbonDatasetPiece(object):
    def __init__(self, path):
        self.path = path
        #TODO get record count from carbonapp based on file
        self.num_rows = 10000
    def read_all(self, columns):
        carbon_reader = _CarbonReader(path=self.path)
        return carbon_reader.read(columns)
