import time

import pyarrow as pa

class CarbonReader(object):
    def __init__(self):
        from jnius import autoclass
        self.readerClass = autoclass('org.apache.carbondata.sdk.file.CarbonReader')

    def builder(self):
        self.CarbonReaderBuilder = self.readerClass.builder()
        return self

    def withFile(self, fileName):
        self.CarbonReaderBuilder.withFile(fileName)
        return self

    def withFileLists(self, fileLists):
        self.CarbonReaderBuilder.withFileLists(fileLists)
        return self

    def withFolder(self, fileName):
        self.CarbonReaderBuilder.withFolder(fileName)
        return self

    def withBatch(self, batchSize):
        self.CarbonReaderBuilder.withBatch(batchSize)
        return self

    def projection(self, projection_list):
        self.CarbonReaderBuilder.projection(projection_list)
        return self

    def filterEqual(self, columnName, value):
        self.CarbonReaderBuilder.filterEqual(columnName, value)
        return self

    def withHadoopConf(self, key, value):
        self.CarbonReaderBuilder.withHadoopConf(key, value)
        return self

    def build(self):
        self.reader = self.CarbonReaderBuilder.build()
        return self

    def build_with_split(self, input_split):
        self.reader = self.CarbonReaderBuilder.buildWithSplits(input_split)
        return self

    def splitAsArray(self, maxSplits):
        return self.reader.splitAsArray(maxSplits)

    def hasNext(self):
        return self.reader.hasNext()

    def readNextRow(self):
        return self.reader.readNextRow()

    def readNextBatchRow(self):
        return self.reader.readNextBatchRow()

    # for petastorm integration
    def readArrowBatch(self, schema):
        return self.reader.readArrowBatch(schema)

    def getSplits(self):
        return self.CarbonReaderBuilder.getSplits()

    def read(self, schema):
        # start = time.time()
        buf = self.reader.readArrowBatch(schema).tostring()
        # print("arrow batch time " + str(time.time() - start))
        # start = time.time()
        reader = pa.RecordBatchFileReader(pa.BufferReader(bytes(buf)))
        data = reader.read_all()
        # print("arrow conversion time " + str(time.time() - start))
        return data

    def close(self):
        return self.reader.close()
