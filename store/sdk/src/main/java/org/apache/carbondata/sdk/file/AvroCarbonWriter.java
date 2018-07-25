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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.StructField;
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
        out = fieldValue;
        break;
      case FLOAT:
        // direct conversion will change precision. So parse from string.
        // also carbon internally needs float as double
        out = Double.parseDouble(fieldValue.toString());
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
    String FieldName = avroField.name();
    Schema childSchema = avroField.schema();
    Schema.Type type = childSchema.getType();
    LogicalType logicalType = childSchema.getLogicalType();
    switch (type) {
      case BOOLEAN:
        return new Field(FieldName, DataTypes.BOOLEAN);
      case INT:
        if (logicalType instanceof LogicalTypes.Date) {
          return new Field(FieldName, DataTypes.DATE);
        } else {
          LOGGER.warn("Unsupported logical type. Considering Data Type as INT for " + childSchema
              .getName());
          return new Field(FieldName, DataTypes.INT);
        }
      case LONG:
        if (logicalType instanceof LogicalTypes.TimestampMillis
            || logicalType instanceof LogicalTypes.TimestampMicros) {
          return new Field(FieldName, DataTypes.TIMESTAMP);
        } else {
          LOGGER.warn("Unsupported logical type. Considering Data Type as LONG for " + childSchema
              .getName());
          return new Field(FieldName, DataTypes.LONG);
        }
      case DOUBLE:
        return new Field(FieldName, DataTypes.DOUBLE);
      case STRING:
        return new Field(FieldName, DataTypes.STRING);
      case FLOAT:
        return new Field(FieldName, DataTypes.DOUBLE);
      case RECORD:
        // recursively get the sub fields
        ArrayList<StructField> structSubFields = new ArrayList<>();
        for (Schema.Field avroSubField : childSchema.getFields()) {
          StructField structField = prepareSubFields(avroSubField.name(), avroSubField.schema());
          if (structField != null) {
            structSubFields.add(structField);
          }
        }
        return new Field(FieldName, "struct", structSubFields);
      case ARRAY:
        // recursively get the sub fields
        ArrayList<StructField> arraySubField = new ArrayList<>();
        // array will have only one sub field.
        StructField structField = prepareSubFields("val", childSchema.getElementType());
        if (structField != null) {
          arraySubField.add(structField);
          return new Field(FieldName, "array", arraySubField);
        } else {
          return null;
        }
      case NULL:
        return null;
      default:
        throw new UnsupportedOperationException(
            "carbon not support " + type.toString() + " avro type yet");
    }
  }

  private static StructField prepareSubFields(String FieldName, Schema childSchema) {
    Schema.Type type = childSchema.getType();
    LogicalType logicalType = childSchema.getLogicalType();
    switch (type) {
      case BOOLEAN:
        return new StructField(FieldName, DataTypes.BOOLEAN);
      case INT:
        if (logicalType instanceof LogicalTypes.Date) {
          return new StructField(FieldName, DataTypes.DATE);
        } else {
          LOGGER.warn("Unsupported logical type. Considering Data Type as INT for " + childSchema
              .getName());
          return new StructField(FieldName, DataTypes.INT);
        }
      case LONG:
        if (logicalType instanceof LogicalTypes.TimestampMillis
            || logicalType instanceof LogicalTypes.TimestampMicros) {
          return new StructField(FieldName, DataTypes.TIMESTAMP);
        } else {
          LOGGER.warn("Unsupported logical type. Considering Data Type as LONG for " + childSchema
              .getName());
          return new StructField(FieldName, DataTypes.LONG);
        }
      case DOUBLE:
        return new StructField(FieldName, DataTypes.DOUBLE);
      case STRING:
        return new StructField(FieldName, DataTypes.STRING);
      case FLOAT:
        return new StructField(FieldName, DataTypes.DOUBLE);
      case RECORD:
        // recursively get the sub fields
        ArrayList<StructField> structSubFields = new ArrayList<>();
        for (Schema.Field avroSubField : childSchema.getFields()) {
          StructField structField = prepareSubFields(avroSubField.name(), avroSubField.schema());
          if (structField != null) {
            structSubFields.add(structField);
          }
        }
        return (new StructField(FieldName, DataTypes.createStructType(structSubFields)));
      case ARRAY:
        // recursively get the sub fields
        // array will have only one sub field.
        DataType subType = getMappingDataTypeForArrayRecord(childSchema.getElementType());
        if (subType != null) {
          return (new StructField(FieldName, DataTypes.createArrayType(subType)));
        } else {
          return null;
        }
      case NULL:
        return null;
      default:
        throw new UnsupportedOperationException(
            "carbon not support " + type.toString() + " avro type yet");
    }
  }

  private static DataType getMappingDataTypeForArrayRecord(Schema childSchema) {
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
      case STRING:
        return DataTypes.STRING;
      case FLOAT:
        return DataTypes.DOUBLE;
      case RECORD:
        // recursively get the sub fields
        ArrayList<StructField> structSubFields = new ArrayList<>();
        for (Schema.Field avroSubField : childSchema.getFields()) {
          StructField structField = prepareSubFields(avroSubField.name(), avroSubField.schema());
          if (structField != null) {
            structSubFields.add(structField);
          }
        }
        return DataTypes.createStructType(structSubFields);
      case ARRAY:
        // array will have only one sub field.
        DataType subType = getMappingDataTypeForArrayRecord(childSchema.getElementType());
        if (subType != null) {
          return DataTypes.createArrayType(subType);
        } else {
          return null;
        }
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
