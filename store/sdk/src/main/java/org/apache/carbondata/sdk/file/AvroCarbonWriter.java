/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.MapType;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.datatype.StructType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.processing.loading.complexobjects.ArrayObject;
import org.apache.carbondata.processing.loading.complexobjects.StructObject;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

/**
 * Writer Implementation to write Avro Record to carbondata file.
 */
@InterfaceAudience.Internal
public class AvroCarbonWriter extends CarbonWriter {

  private RecordWriter<NullWritable, ObjectArrayWritable> recordWriter;
  private TaskAttemptContext context;
  private ObjectArrayWritable writable;
  private Schema avroSchema;
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonTable.class.getName());

  AvroCarbonWriter(CarbonLoadModel loadModel) throws IOException {
    Configuration hadoopConf = new Configuration();
    CarbonTableOutputFormat.setLoadModel(hadoopConf, loadModel);
    CarbonTableOutputFormat format = new CarbonTableOutputFormat();
    JobID jobId = new JobID(UUID.randomUUID().toString(), 0);
    Random random = new Random();
    TaskID task = new TaskID(jobId, TaskType.MAP, random.nextInt());
    TaskAttemptID attemptID = new TaskAttemptID(task, random.nextInt());
    TaskAttemptContextImpl context = new TaskAttemptContextImpl(hadoopConf, attemptID);
    this.recordWriter = format.getRecordWriter(context);
    this.context = context;
    this.writable = new ObjectArrayWritable();
  }

  private Object[] avroToCsv(GenericData.Record avroRecord) {
    if (avroSchema == null) {
      avroSchema = avroRecord.getSchema();
    }
    List<Schema.Field> fields = avroSchema.getFields();
    List<Object> csvFields = new ArrayList<>();
    for (int i = 0; i < fields.size(); i++) {
      Object field = avroFieldToObject(fields.get(i), avroRecord.get(i));
      if (field != null) {
        csvFields.add(field);
      }
    }
    return csvFields.toArray();
  }

  private Object avroFieldToObject(Schema.Field avroField, Object fieldValue) {
    Object out;
    Schema.Type type = avroField.schema().getType();
    LogicalType logicalType = avroField.schema().getLogicalType();
    switch (type) {
      case MAP:
        // Note: Avro object takes care of removing the duplicates so we should not handle it again
        // Map will be internally stored as Array<Struct<Key,Value>>
        Map mapEntries = (HashMap) fieldValue;
        Object[] arrayMapChildObjects = new Object[mapEntries.size()];
        if (!mapEntries.isEmpty()) {
          Iterator iterator = mapEntries.entrySet().iterator();
          int counter = 0;
          while (iterator.hasNext()) {
            // size is 2 because map will have key and value
            Object[] mapChildObjects = new Object[2];
            Map.Entry mapEntry = (HashMap.Entry) iterator.next();
            // evaluate key
            Object keyObject = avroFieldToObject(
                new Schema.Field(avroField.name(), Schema.create(Schema.Type.STRING),
                    avroField.doc(), avroField.defaultVal()), mapEntry.getKey());
            // evaluate value
            Object valueObject = avroFieldToObject(
                new Schema.Field(avroField.name(), avroField.schema().getValueType(),
                    avroField.doc(), avroField.defaultVal()), mapEntry.getValue());
            if (keyObject != null) {
              mapChildObjects[0] = keyObject;
            }
            if (valueObject != null) {
              mapChildObjects[1] = valueObject;
            }
            StructObject keyValueObject = new StructObject(mapChildObjects);
            arrayMapChildObjects[counter++] = keyValueObject;
          }
        }
        out = new ArrayObject(arrayMapChildObjects);
        break;
      case RECORD:
        List<Schema.Field> fields = avroField.schema().getFields();

        Object[] structChildObjects = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          Object childObject =
              avroFieldToObject(fields.get(i), ((GenericData.Record) fieldValue).get(i));
          if (childObject != null) {
            structChildObjects[i] = childObject;
          }
        }
        StructObject structObject = new StructObject(structChildObjects);
        out = structObject;
        break;
      case ARRAY:
        Object[] arrayChildObjects;
        if (fieldValue instanceof GenericData.Array) {
          int size = ((GenericData.Array) fieldValue).size();
          arrayChildObjects = new Object[size];
          for (int i = 0; i < size; i++) {
            Object childObject = avroFieldToObject(
                new Schema.Field(avroField.name(), avroField.schema().getElementType(),
                    avroField.doc(), avroField.defaultVal()),
                ((GenericData.Array) fieldValue).get(i));
            if (childObject != null) {
              arrayChildObjects[i] = childObject;
            }
          }
        } else {
          int size = ((ArrayList) fieldValue).size();
          arrayChildObjects = new Object[size];
          for (int i = 0; i < size; i++) {
            Object childObject = avroFieldToObject(
                new Schema.Field(avroField.name(), avroField.schema().getElementType(),
                    avroField.doc(), avroField.defaultVal()), ((ArrayList) fieldValue).get(i));
            if (childObject != null) {
              arrayChildObjects[i] = childObject;
            }
          }
        }
        out = new ArrayObject(arrayChildObjects);
        break;
      case UNION:
        // Union type will be internally stored as Struct<col:type>
        // Fill data object only if fieldvalue is instance of datatype
        // For other field datatypes, fill value as Null
        List<Schema> unionFields = avroField.schema().getTypes();
        int notNullUnionFieldsCount = 0;
        for (Schema unionField : unionFields) {
          if (!unionField.getType().equals(Schema.Type.NULL)) {
            notNullUnionFieldsCount++;
          }
        }
        Object[] values = new Object[notNullUnionFieldsCount];
        int j = 0;
        for (Schema unionField : unionFields) {
          if (unionField.getType().equals(Schema.Type.NULL)) {
            continue;
          }
          if (checkFieldValueType(unionField.getType(), fieldValue)) {
            values[j] = avroFieldToObjectForUnionType(unionField, fieldValue, avroField);
            break;
          }
          j++;
        }
        out = new StructObject(values);
        break;
      default:
        out = avroPrimitiveFieldToObject(type, logicalType, fieldValue);
    }
    return out;
  }

  /**
   * For Union type, fill data if Schema.Type is instance of fieldValue
   * and return result
   *
   * @param type
   * @param fieldValue
   * @return
   */
  private boolean checkFieldValueType(Schema.Type type, Object fieldValue) {
    switch (type) {
      case INT:
        return (fieldValue instanceof Integer);
      case BOOLEAN:
        return (fieldValue instanceof Boolean);
      case LONG:
        return (fieldValue instanceof Long);
      case DOUBLE:
        return (fieldValue instanceof Double);
      case STRING:
        return (fieldValue instanceof Utf8 || fieldValue instanceof String);
      case FLOAT:
        return (fieldValue instanceof Float);
      case RECORD:
        return (fieldValue instanceof GenericData.Record);
      case ARRAY:
        return (fieldValue instanceof GenericData.Array || fieldValue instanceof ArrayList);
      case BYTES:
        return (fieldValue instanceof ByteBuffer);
      case MAP:
        return (fieldValue instanceof HashMap);
      case ENUM:
        return (fieldValue instanceof GenericData.EnumSymbol);
      default:
        return false;
    }
  }

  private Object avroPrimitiveFieldToObject(Schema.Type type, LogicalType logicalType,
      Object fieldValue) {
    Object out = null;
    switch (type) {
      case INT:
        if (logicalType != null) {
          if (logicalType instanceof LogicalTypes.Date) {
            int dateIntValue = (int) fieldValue;
            out = dateIntValue * DateDirectDictionaryGenerator.MILLIS_PER_DAY;
          } else {
            LOGGER.warn("Actual type: INT, Logical Type: " + logicalType.getName());
            out = fieldValue;
          }
        } else {
          out = fieldValue;
        }
        break;
      case BOOLEAN:
      case LONG:
        if (logicalType != null && !(logicalType instanceof LogicalTypes.TimestampMillis)) {
          if (logicalType instanceof LogicalTypes.TimestampMicros) {
            long dateIntValue = (long) fieldValue;
            out = dateIntValue / 1000L;
          } else {
            LOGGER.warn("Actual type: INT, Logical Type: " + logicalType.getName());
            out = fieldValue;
          }
        } else {
          out = fieldValue;
        }
        break;
      case DOUBLE:
      case STRING:
      case ENUM:
        out = fieldValue;
        break;
      case FLOAT:
        // direct conversion will change precision. So parse from string.
        // also carbon internally needs float as double
        out = Double.parseDouble(fieldValue.toString());
        break;
      case BYTES:
        // DECIMAL type is defined in Avro as a BYTE type with the logicalType property
        // set to "decimal" and a specified precision and scale
        // As binary type is not supported yet,value will be null
        if (logicalType instanceof LogicalTypes.Decimal) {
          out = new BigDecimal(new String(((ByteBuffer) fieldValue).array(),
              CarbonCommonConstants.DEFAULT_CHARSET_CLASS));
        }
        break;
      case NULL:
        out = null;
        break;
      default:
        throw new UnsupportedOperationException(
            "carbon not support " + type.toString() + " avro type yet");
    }
    return out;
  }

  /**
   * fill fieldvalue for union type
   *
   * @param avroField
   * @param fieldValue
   * @param avroFields
   * @return
   */
  private Object avroFieldToObjectForUnionType(Schema avroField, Object fieldValue,
      Schema.Field avroFields) {
    Object out;
    Schema.Type type = avroField.getType();
    LogicalType logicalType = avroField.getLogicalType();
    switch (type) {
      case RECORD:
        if (fieldValue instanceof GenericData.Record) {
          List<Schema.Field> fields = avroField.getFields();

          Object[] structChildObjects = new Object[fields.size()];
          for (int i = 0; i < fields.size(); i++) {
            Object childObject =
                avroFieldToObject(fields.get(i), ((GenericData.Record) fieldValue).get(i));
            if (childObject != null) {
              structChildObjects[i] = childObject;
            }
          }
          out = new StructObject(structChildObjects);
        } else {
          out = null;
        }
        break;
      case ARRAY:
        if (fieldValue instanceof GenericData.Array || fieldValue instanceof ArrayList) {
          Object[] arrayChildObjects;
          if (fieldValue instanceof GenericData.Array) {
            int size = ((GenericData.Array) fieldValue).size();
            arrayChildObjects = new Object[size];
            for (int i = 0; i < size; i++) {
              Object childObject = avroFieldToObject(
                  new Schema.Field(avroFields.name(), avroField.getElementType(), avroFields.doc(),
                      avroFields.defaultVal()), ((GenericData.Array) fieldValue).get(i));
              if (childObject != null) {
                arrayChildObjects[i] = childObject;
              }
            }
          } else {
            int size = ((ArrayList) fieldValue).size();
            arrayChildObjects = new Object[size];
            for (int i = 0; i < size; i++) {
              Object childObject = avroFieldToObject(
                  new Schema.Field(avroFields.name(), avroField.getElementType(), avroFields.doc(),
                      avroFields.defaultVal()), ((ArrayList) fieldValue).get(i));
              if (childObject != null) {
                arrayChildObjects[i] = childObject;
              }
            }
          }
          out = new ArrayObject(arrayChildObjects);
        } else {
          out = null;
        }
        break;
      case MAP:
        // Note: Avro object takes care of removing the duplicates so we should not handle it again
        // Map will be internally stored as Array<Struct<Key,Value>>
        if (fieldValue instanceof HashMap) {
          Map mapEntries = (HashMap) fieldValue;
          Object[] arrayMapChildObjects = new Object[mapEntries.size()];
          if (!mapEntries.isEmpty()) {
            Iterator iterator = mapEntries.entrySet().iterator();
            int counter = 0;
            while (iterator.hasNext()) {
              // size is 2 because map will have key and value
              Object[] mapChildObjects = new Object[2];
              Map.Entry mapEntry = (HashMap.Entry) iterator.next();
              // evaluate key
              Object keyObject = avroFieldToObject(
                  new Schema.Field(avroFields.name(), Schema.create(Schema.Type.STRING),
                      avroFields.doc(), avroFields.defaultVal()), mapEntry.getKey());
              // evaluate value
              Object valueObject = avroFieldToObject(
                  new Schema.Field(avroFields.name(), avroField.getValueType(), avroFields.doc(),
                      avroFields.defaultVal()), mapEntry.getValue());
              if (keyObject != null) {
                mapChildObjects[0] = keyObject;
              }
              if (valueObject != null) {
                mapChildObjects[1] = valueObject;
              }
              StructObject keyValueObject = new StructObject(mapChildObjects);
              arrayMapChildObjects[counter++] = keyValueObject;
            }
          }
          out = new ArrayObject(arrayMapChildObjects);
        } else {
          out = null;
        }
        break;
      default:
        out = avroPrimitiveFieldToObject(type, logicalType, fieldValue);
    }
    return out;
  }

  /**
   * converts avro schema to carbon schema required by carbonWriter
   *
   * @param avroSchema avro schema
   * @return carbon sdk schema
   */
  public static org.apache.carbondata.sdk.file.Schema getCarbonSchemaFromAvroSchema(
      Schema avroSchema) {
    Field[] carbonField = new Field[avroSchema.getFields().size()];
    int i = 0;
    for (Schema.Field avroField : avroSchema.getFields()) {
      Field field = prepareFields(avroField);
      if (field != null) {
        carbonField[i] = field;
      }
      i++;
    }
    return new org.apache.carbondata.sdk.file.Schema(carbonField);
  }

  private static Field prepareFields(Schema.Field avroField) {
    String fieldName = avroField.name();
    Schema childSchema = avroField.schema();
    Schema.Type type = childSchema.getType();
    LogicalType logicalType = childSchema.getLogicalType();
    switch (type) {
      case BOOLEAN:
        return new Field(fieldName, DataTypes.BOOLEAN);
      case INT:
        if (logicalType instanceof LogicalTypes.Date) {
          return new Field(fieldName, DataTypes.DATE);
        } else {
          LOGGER.warn("Unsupported logical type. Considering Data Type as INT for " + childSchema
              .getName());
          return new Field(fieldName, DataTypes.INT);
        }
      case LONG:
        if (logicalType instanceof LogicalTypes.TimestampMillis
            || logicalType instanceof LogicalTypes.TimestampMicros) {
          return new Field(fieldName, DataTypes.TIMESTAMP);
        } else {
          LOGGER.warn("Unsupported logical type. Considering Data Type as LONG for " + childSchema
              .getName());
          return new Field(fieldName, DataTypes.LONG);
        }
      case DOUBLE:
        return new Field(fieldName, DataTypes.DOUBLE);
      case ENUM:
      case STRING:
        return new Field(fieldName, DataTypes.STRING);
      case FLOAT:
        return new Field(fieldName, DataTypes.DOUBLE);
      case MAP:
        // recursively get the sub fields
        ArrayList<Field> mapSubFields = new ArrayList<>();
        Field mapField = prepareSubFields("val", childSchema);
        if (null != mapField) {
          // key value field will be wrapped inside a map struct field
          Field keyValueField = mapField.getChildren().get(0);
          // value dataType will be at position 1 in the fields
          DataType valueType =
              ((StructType) keyValueField.getDataType()).getFields().get(1).getDataType();
          MapType mapType = DataTypes.createMapType(DataTypes.STRING, valueType);
          mapSubFields.add(keyValueField);
          return new Field(fieldName, mapType, mapSubFields);
        }
        return null;
      case RECORD:
        // recursively get the sub fields
        ArrayList<Field> structSubFields = new ArrayList<>();
        for (Schema.Field avroSubField : childSchema.getFields()) {
          Field structField = prepareSubFields(avroSubField.name(), avroSubField.schema());
          if (structField != null) {
            structSubFields.add(structField);
          }
        }
        return new Field(fieldName, "struct", structSubFields);
      case ARRAY:
        // recursively get the sub fields
        ArrayList<Field> arraySubField = new ArrayList<>();
        // array will have only one sub field.
        Field structField = prepareSubFields("val", childSchema.getElementType());
        if (structField != null) {
          arraySubField.add(structField);
          return new Field(fieldName, "array", arraySubField);
        } else {
          return null;
        }
      case UNION:
        int i = 0;
        // Get union types and store as Struct<type>
        ArrayList<Field> unionFields = new ArrayList<>();
        for (Schema avroSubField : avroField.schema().getTypes()) {
          if (!avroSubField.getType().equals(Schema.Type.NULL)) {
            Field unionField = prepareSubFields(avroField.name() + i++, avroSubField);
            if (unionField != null) {
              unionFields.add(unionField);
            }
          }
        }
        if (unionFields.isEmpty()) {
          throw new UnsupportedOperationException(
              "Carbon do not support Avro UNION with only null type");
        }
        return new Field(fieldName, "struct", unionFields);
      case BYTES:
        // DECIMAL type is defined in Avro as a BYTE type with the logicalType property
        // set to "decimal" and a specified precision and scale
        if (logicalType instanceof LogicalTypes.Decimal) {
          int precision = ((LogicalTypes.Decimal) childSchema.getLogicalType()).getPrecision();
          int scale = ((LogicalTypes.Decimal) childSchema.getLogicalType()).getScale();
          return new Field(fieldName, DataTypes.createDecimalType(precision, scale));
        }
        return null;
      case NULL:
        return null;
      default:
        throw new UnsupportedOperationException(
            "carbon not support " + type.toString() + " avro type yet");
    }
  }

  private static Field prepareSubFields(String fieldName, Schema childSchema) {
    Schema.Type type = childSchema.getType();
    LogicalType logicalType = childSchema.getLogicalType();
    switch (type) {
      case BOOLEAN:
        return new Field(fieldName, DataTypes.BOOLEAN);
      case INT:
        if (logicalType instanceof LogicalTypes.Date) {
          return new Field(fieldName, DataTypes.DATE);
        } else {
          LOGGER.warn("Unsupported logical type. Considering Data Type as INT for " + childSchema
              .getName());
          return new Field(fieldName, DataTypes.INT);
        }
      case LONG:
        if (logicalType instanceof LogicalTypes.TimestampMillis
            || logicalType instanceof LogicalTypes.TimestampMicros) {
          return new Field(fieldName, DataTypes.TIMESTAMP);
        } else {
          LOGGER.warn("Unsupported logical type. Considering Data Type as LONG for " + childSchema
              .getName());
          return new Field(fieldName, DataTypes.LONG);
        }
      case DOUBLE:
        return new Field(fieldName, DataTypes.DOUBLE);
      case ENUM:
      case STRING:
        return new Field(fieldName, DataTypes.STRING);
      case FLOAT:
        return new Field(fieldName, DataTypes.DOUBLE);
      case MAP:
        // recursively get the sub fields
        ArrayList<Field> keyValueFields = new ArrayList<>();
        // for Avro key dataType is always fixed as String
        Field keyField = new Field(fieldName + ".key", DataTypes.STRING);
        Field valueField = prepareSubFields(fieldName + ".value", childSchema.getValueType());
        if (null != valueField) {
          keyValueFields.add(keyField);
          keyValueFields.add(valueField);
          Field mapKeyValueField =
              new Field(fieldName, createStructType(keyValueFields));
          // value dataType will be at position 1 in the fields
          MapType mapType =
              DataTypes.createMapType(DataTypes.STRING, mapKeyValueField.getDataType());
          List<Field> mapStructFields = new ArrayList<>();
          mapStructFields.add(mapKeyValueField);
          return new Field(fieldName, mapType, mapStructFields);
        }
        return null;
      case RECORD:
        // recursively get the sub fields
        ArrayList<Field> structSubFields = new ArrayList<>();
        for (Schema.Field avroSubField : childSchema.getFields()) {
          Field structField = prepareSubFields(avroSubField.name(), avroSubField.schema());
          if (structField != null) {
            structSubFields.add(structField);
          }
        }
        return (new Field(fieldName, createStructType(structSubFields)));
      case ARRAY:
        // recursively get the sub fields
        // array will have only one sub field.
        DataType subType = getMappingDataTypeForCollectionRecord(childSchema.getElementType());
        if (subType != null) {
          return (new Field(fieldName, DataTypes.createArrayType(subType)));
        } else {
          return null;
        }
      case UNION:
        // recursively get the union types
        int i = 0;
        ArrayList<Field> structSubTypes = new ArrayList<>();
        for (Schema avroSubField : childSchema.getTypes()) {
          Field structField = prepareSubFields(fieldName + i++, avroSubField);
          if (structField != null) {
            structSubTypes.add(structField);
          }
        }
        return (new Field(fieldName, createStructType(structSubTypes)));
      case BYTES:
        // DECIMAL type is defined in Avro as a BYTE type with the logicalType property
        // set to "decimal" and a specified precision and scale
        if (logicalType instanceof LogicalTypes.Decimal) {
          int precision = ((LogicalTypes.Decimal) childSchema.getLogicalType()).getPrecision();
          int scale = ((LogicalTypes.Decimal) childSchema.getLogicalType()).getScale();
          return new Field(fieldName, DataTypes.createDecimalType(precision, scale));
        }
        return null;
      case NULL:
        return null;
      default:
        throw new UnsupportedOperationException(
            "carbon not support " + type.toString() + " avro type yet");
    }
  }

  private static StructType createStructType(List<Field> fields) {
    List<StructField> f = fields.stream().map(field ->
        getStructField(field)
    ).collect(Collectors.toList());
    return DataTypes.createStructType(f);
  }

  /**
   * convert Field to StructField
   *
   * @param field Field object for SDK
   * @return StructField object of Carbon
   */
  private static StructField getStructField(Field field) {
    if (field.getChildren() != null) {
      return new StructField(field.getFieldName(), field.getDataType(),
          createStructType(field.getChildren()).getFields());
    } else {
      return new StructField(field.getFieldName(), field.getDataType());
    }
  }

  private static DataType getMappingDataTypeForCollectionRecord(Schema childSchema) {
    LogicalType logicalType = childSchema.getLogicalType();
    switch (childSchema.getType()) {
      case BOOLEAN:
        return DataTypes.BOOLEAN;
      case INT:
        if (logicalType != null) {
          if (logicalType instanceof LogicalTypes.Date) {
            return DataTypes.DATE;
          } else {
            LOGGER.warn("Unsupported logical type. Considering Data Type as INT for " + childSchema
                .getName());
            return DataTypes.INT;
          }
        } else {
          return DataTypes.INT;
        }
      case LONG:
        if (logicalType != null) {
          if (logicalType instanceof LogicalTypes.TimestampMillis
              || logicalType instanceof LogicalTypes.TimestampMicros) {
            return DataTypes.TIMESTAMP;
          } else {
            LOGGER.warn("Unsupported logical type. Considering Data Type as LONG for " + childSchema
                .getName());
            return DataTypes.LONG;
          }
        } else {
          return DataTypes.LONG;
        }
      case DOUBLE:
        return DataTypes.DOUBLE;
      case ENUM:
      case STRING:
        return DataTypes.STRING;
      case FLOAT:
        return DataTypes.DOUBLE;
      case MAP:
        // recursively get the sub fields
        Field mapField = prepareSubFields("val", childSchema);
        if (mapField != null) {
          DataType ketType = ((StructType) mapField.getDataType()).getFields().get(0).getDataType();
          DataType valueType =
              ((StructType) mapField.getDataType()).getFields().get(1).getDataType();
          return DataTypes.createMapType(ketType, valueType);
        }
        return null;
      case RECORD:
        // recursively get the sub fields
        ArrayList<Field> structSubFields = new ArrayList<>();
        for (Schema.Field avroSubField : childSchema.getFields()) {
          Field structField = prepareSubFields(avroSubField.name(), avroSubField.schema());
          if (structField != null) {
            structSubFields.add(structField);
          }
        }
        return createStructType(structSubFields);
      case ARRAY:
        // array will have only one sub field.
        DataType subType = getMappingDataTypeForCollectionRecord(childSchema.getElementType());
        if (subType != null) {
          return DataTypes.createArrayType(subType);
        } else {
          return null;
        }
      case UNION:
        int i = 0;
        // recursively get the union types and create struct type
        ArrayList<Field> unionFields = new ArrayList<>();
        for (Schema avroSubField : childSchema.getTypes()) {
          Field unionField = prepareSubFields(avroSubField.getName() + i++, avroSubField);
          if (unionField != null) {
            unionFields.add(unionField);
          }
        }
        return createStructType(unionFields);
      case BYTES:
        // DECIMAL type is defined in Avro as a BYTE type with the logicalType property
        // set to "decimal" and a specified precision and scale
        if (logicalType instanceof LogicalTypes.Decimal) {
          int precision = ((LogicalTypes.Decimal) childSchema.getLogicalType()).getPrecision();
          int scale = ((LogicalTypes.Decimal) childSchema.getLogicalType()).getScale();
          return DataTypes.createDecimalType(precision, scale);
        }
        return null;
      case NULL:
        return null;
      default:
        throw new UnsupportedOperationException(
            "carbon not support " + childSchema.getType().toString() + " avro type yet");
    }
  }

  /**
   * Write single row data, input row is Avro Record
   */
  @Override
  public void write(Object object) throws IOException {
    try {
      GenericData.Record record = (GenericData.Record) object;

      // convert Avro record to CSV String[]
      Object[] csvRecord = avroToCsv(record);
      writable.set(csvRecord);
      recordWriter.write(NullWritable.get(), writable);
    } catch (Exception e) {
      close();
      throw new IOException(e);
    }
  }

  /**
   * Flush and close the writer
   */
  @Override public void close() throws IOException {
    try {
      recordWriter.close(context);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
