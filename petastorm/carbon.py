import os

import pyarrow as pa
from pyarrow.filesystem import (_get_fs_from_path)
from pyarrow.parquet import ParquetFile
from pycarbon.CarbonReader import CarbonReader
from pycarbon.CarbonSchemaReader import CarbonSchemaReader


class CarbonDataset(object):
    def __init__(self, path):
        self.path = path
        self.fs = _get_fs_from_path(path)
        self.pieces = list()
        carbon_splits = CarbonReader().builder().withFolder(self.path).getSplits()
        carbon_schema = CarbonSchemaReader().readSchema(self.path)
        for split in carbon_splits:
            self.pieces.append(CarbonDatasetPiece(path, carbon_schema, split))
        self.number_of_splits = len(self.pieces)
        self.schema = self.getArrowSchema()
        # TODO add mechanism to get the file path based on file filter
        self.common_metadata_path = path + '/_common_metadata'
        self.common_metadata = None
        try:
            if self.fs.exists(self.common_metadata_path):
                with self.fs.open(self.common_metadata_path) as f:
                    self.common_metadata = ParquetFile(f).metadata
        except:
            self.common_metadata = None

    def getArrowSchema(self):
        buf = CarbonSchemaReader().readSchema(self.path, True).tostring()
        reader = pa.RecordBatchFileReader(pa.BufferReader(bytes(buf)))
        return reader.read_all().schema


class CarbonDatasetPiece(object):
    def __init__(self, path, carbon_schema, input_split):
        self.path = path
        self.input_split = input_split
        self.carbon_schema = carbon_schema
        # TODO get record count from carbonapp based on file
        self.num_rows = 10000

    def read_all(self, columns):
        # rebuilding the reader as need to read specific columns
        carbon_reader_builder = CarbonReader().builder().withFolder(self.path)
        carbon_schema_reader = CarbonSchemaReader()
        if columns is not None:
            carbon_reader_builder = carbon_reader_builder.projection(columns)
            updatedSchema = carbon_schema_reader.reorderSchemaBasedOnProjection(columns, self.carbon_schema)
        else:
            # TODO Currenlty when projection is not added in carbon reader
            # carbon returns record in dimensions+measures,but here we need based on actual schema order
            # so for handling this adding projection columns based on schema
            updatedSchema = self.carbon_schema
            projection = carbon_schema_reader.getProjectionBasedOnSchema(updatedSchema)
            carbon_reader_builder = carbon_reader_builder.projection(projection)
        carbon_reader = carbon_reader_builder.build_with_split(self.input_split)
        data = carbon_reader.read(updatedSchema)
        carbon_reader.close()
        return data
