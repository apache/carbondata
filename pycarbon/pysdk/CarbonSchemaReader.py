class CarbonSchemaReader(object):
    def __init__(self):
        from jnius import autoclass
        self.carbonSchemaReader = autoclass('org.apache.carbondata.sdk.file.CarbonSchemaReader')
        self.Schema = autoclass('org.apache.carbondata.sdk.file.Schema')

    def readSchema(self, path, getAsBuffer=False, *para):
        if (getAsBuffer == True):
            return self.carbonSchemaReader.getArrowSchemaAsBytes(path)
        if (len(para) == 0):
            schema = self.carbonSchemaReader.readSchema(path)
            newSchema = schema.asOriginOrder()
            return newSchema
        if (len(para) == 1):
            return self.carbonSchemaReader.readSchema(path, para[0]).asOriginOrder()
        if (len(para) == 2):
            return self.carbonSchemaReader.readSchema(path, para[0], para[1]).asOriginOrder()

    def reorderSchemaBasedOnProjection(self, columns, schema):
        fields = schema.getFields()
        updateFields = list()
        for column in columns:
            for field in fields:
                if column.casefold() == field.getFieldName().casefold():
                    updateFields.append(field)
                    break

        updatedSchema = self.Schema(updateFields)
        return updatedSchema

    def getProjectionBasedOnSchema(self, schema):
        fields = schema.getFields()
        projection = list()
        for field in fields:
            projection.append(field.getFieldName())
        return projection