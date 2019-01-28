class CarbonSchemaReader(object):
    def __init__(self, java_entry):
        self.java_entry = java_entry

    def builder(self):
        self.CarbonReaderBuilder = self.jvm.org.apache.carbondata.sdk.file.CarbonSchemaReader()
        return self

    def withFile(self, fileName):
        self.CarbonReaderBuilder.withFile(fileName)
        return self

    def withFileLists(self, fileLists):
        self.CarbonReaderBuilder.withFileLists(fileLists)
        return self

    def withBatch(self, batchSize):
        self.CarbonReaderBuilder.withBatch(batchSize)
        return self

    def withHadoopConf(self, key, value):
        self.CarbonReaderBuilder.withHadoopConf(key, value)
        return self

    def build(self):
        self.reader = self.CarbonReaderBuilder.build()
        return self

    def hasNext(self):
        return self.reader.hasNext()

    def readNextRow(self):
        return self.reader.readNextRow()

    def readNextBatchRow(self):
        return self.reader.readNextBatchRow()

    def close(self):
        return self.reader.close()
