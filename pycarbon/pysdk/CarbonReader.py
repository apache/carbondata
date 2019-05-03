import ctypes

import pyarrow as pa

class CarbonReader(object):
    def __init__(self):
        from jnius import autoclass
        self.readerClass = autoclass('org.apache.carbondata.sdk.file.CarbonReader')

    def builder(self, path):
        self.CarbonReaderBuilder = self.readerClass.builder(path)
        return self

    def projection(self, projection_list):
        self.CarbonReaderBuilder.projection(projection_list)
        return self

    def withHadoopConf(self, key, value):
        self.CarbonReaderBuilder.withHadoopConf(key, value)
        return self

    def build_with_split(self, input_split):
        self.reader = self.CarbonReaderBuilder.buildWithSplits(input_split)
        return self

    def getSplits(self):
        return self.CarbonReaderBuilder.getSplits()

    def read(self, schema):
        address = self.reader.readArrowBatchAddress(schema)
        size = (ctypes.c_int32).from_address(address).value
        arrowData = (ctypes.c_byte * size).from_address(address + 4)
        rawData = bytes(arrowData)
        self.reader.freeArrowBatchMemory(address)
        reader = pa.RecordBatchFileReader(pa.BufferReader(rawData))
        data = reader.read_all()
        return data

    def close(self):
        return self.reader.close()
