class CarbonWriter(object):
    def __init__(self):
        from jnius import autoclass
        self.writerClass = autoclass('org.apache.carbondata.sdk.file.CarbonWriter')

    def builder(self):
        self.CarbonWriterBuilder = self.writerClass.builder()
        return self

    def outputPath(self, path):
        self.CarbonWriterBuilder.outputPath(path)
        return self

    def withCsvInput(self, jsonSchema):
        self.CarbonWriterBuilder.withCsvInput(jsonSchema)
        return self

    def writtenBy(self, name):
        self.CarbonWriterBuilder.writtenBy(name)
        return self

    def withHadoopConf(self, key, value):
        self.CarbonWriterBuilder.withHadoopConf(key, value)
        return self

    def build(self):
        self.writer = self.CarbonWriterBuilder.build()
        return self

    def write(self, object):
        return self.writer.write(object)

    def close(self):
        return self.writer.close()
