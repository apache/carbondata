class CarbonSchemaReader(object):
    def __init__(self, java_gate_way):
        self.gateway = java_gate_way
        self.carbonSchemaReader = self.gateway.jvm.org.apache.carbondata.sdk.file.CarbonSchemaReader

    def readSchema(self, path, *para):
        if (len(para) == 0):
            return self.carbonSchemaReader.readSchema(path)
        if (len(para) == 1):
            return self.carbonSchemaReader.readSchema(path, para[0])
        if (len(para) == 2):
            return self.carbonSchemaReader.readSchema(path, para[0], para[1])