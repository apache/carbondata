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
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.hadoop.api.CarbonTableOutputFormat;
import org.apache.carbondata.hadoop.internal.ObjectArrayWritable;
import org.apache.carbondata.processing.loading.complexobjects.ArrayObject;
import org.apache.carbondata.processing.loading.complexobjects.StructObject;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

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
    Object out = null;
    Schema.Type type = avroField.schema().getType();
    switch (type) {
      case BOOLEAN:
      case INT:
      case LONG:
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

        List<Object> structChildObjects = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
          Object childObject =
              avroFieldToObject(fields.get(i), ((GenericData.Record) fieldValue).get(i));
          if (childObject != null) {
            structChildObjects.add(childObject);
          }
        }
        if (structChildObjects.size() > 0) {
          StructObject structObject = new StructObject(structChildObjects.toArray());
          out = structObject;
        }
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
        if (arrayChildObjects.length > 0) {
          out = new ArrayObject(arrayChildObjects);
        }
        break;
      case NULL:
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
   * @param avroSchemaString json formatted avro schema as string
   * @return carbon sdk schema
   */
  public static org.apache.carbondata.sdk.file.Schema getCarbonSchemaFromAvroSchema(
      String avroSchemaString) {
    if (avroSchemaString == null) {
      throw new UnsupportedOperationException("avro schema string cannot be null");
    }
    Schema avroSchema = new Schema.Parser().parse(avroSchemaString);
    List<Field> carbonField = new ArrayList<>();
    for (Schema.Field avroField : avroSchema.getFields()) {
      Field field = prepareFields(avroField);
      if (field != null) {
        carbonField.add(field);
      }
    }
    return new org.apache.carbondata.sdk.file.Schema(carbonField.toArray(new Field[0]));
  }

  private static Field prepareFields(Schema.Field avroField) {
    String FieldName = avroField.name();
    Schema childSchema = avroField.schema();
    Schema.Type type = childSchema.getType();
    switch (type) {
      case BOOLEAN:
        return new Field(FieldName, DataTypes.BOOLEAN);
      case INT:
        return new Field(FieldName, DataTypes.INT);
      case LONG:
        return new Field(FieldName, DataTypes.LONG);
      case DOUBLE:
        return new Field(FieldName, DataTypes.DOUBLE);
      case STRING:
        return new Field(FieldName, DataTypes.STRING);
      case FLOAT:
        return new Field(FieldName, DataTypes.DOUBLE);
      case RECORD:
        // recursively get the sub fields
        ArrayList<Field> structSubFields = new ArrayList<>();
        for (Schema.Field avroSubField : childSchema.getFields()) {
          StructField structField = prepareSubFields(avroSubField.name(), avroSubField.schema());
          if (structField != null) {
            structSubFields.add(structField);
          }
        }
        if (structSubFields.size() > 0) {
          return new Field(FieldName, "struct", structSubFields);
        } else {
          return null;
        }
      case ARRAY:
        // recursively get the sub fields
        ArrayList<Field> arraySubField = new ArrayList<>();
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
    switch (type) {
      case BOOLEAN:
        return new StructField(FieldName, DataTypes.BOOLEAN);
      case INT:
        return new StructField(FieldName, DataTypes.INT);
      case LONG:
        return new StructField(FieldName, DataTypes.LONG);
      case DOUBLE:
        return new StructField(FieldName, DataTypes.DOUBLE);
      case STRING:
        return new StructField(FieldName, DataTypes.STRING);
      case FLOAT:
        return new StructField(FieldName, DataTypes.DOUBLE);
      case RECORD:
        // recursively get the sub fields
        ArrayList<Field> structSubFields = new ArrayList<>();
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
    switch (childSchema.getType()) {
      case BOOLEAN:
        return DataTypes.BOOLEAN;
      case INT:
        return DataTypes.INT;
      case LONG:
        return DataTypes.LONG;
      case DOUBLE:
        return DataTypes.DOUBLE;
      case STRING:
        return DataTypes.STRING;
      case FLOAT:
        return DataTypes.DOUBLE;
      case RECORD:
        // recursively get the sub fields
        ArrayList<Field> structSubFields = new ArrayList<>();
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
