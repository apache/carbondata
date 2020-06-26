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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.metadata.datatype.MapType;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.orc.FileFormatException;
import org.apache.orc.TypeDescription;


/**
 * Implementation to write ORC rows in CSV format to carbondata file.
 */
public class ORCCarbonWriter extends CarbonWriter {
  private Configuration configuration;
  private Reader orcReader = null;
  private CarbonFile[] dataFiles;
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ORCCarbonWriter.class.getName());
  private CSVCarbonWriter csvCarbonWriter;

  ORCCarbonWriter(CarbonLoadModel loadModel, Configuration hadoopConf)
      throws IOException {
    this.csvCarbonWriter = new CSVCarbonWriter(loadModel, hadoopConf);
    this.configuration = hadoopConf;
  }

  @Override
  public void setDataFiles(CarbonFile[] dataFiles) throws IOException {
    if (dataFiles == null || dataFiles.length == 0) {
      throw new RuntimeException("data files can't be empty.");
    }
    this.compareAllOrcFilesSchema(dataFiles);
    this.dataFiles = dataFiles;
  }

  private void compareAllOrcFilesSchema(CarbonFile[] dataFiles) throws IOException {
    TypeDescription orcSchema = null;
    for (CarbonFile dataFile : dataFiles) {
      Reader orcReader = buildOrcReader(dataFile.getPath(), this.configuration);
      if (orcSchema == null) {
        orcSchema = orcReader.getSchema();
      } else {
        if (!orcSchema.toString().equals(orcReader.getSchema().toString())) {
          throw new RuntimeException("All the ORC files must be having the same schema.");
        }
      }
    }
  }

  // extract child schema from the ORC type description.
  private static Field[] childSchema(Field[] childs,
      List<TypeDescription> childSchemas, List<String> fieldsName) {
    if (childSchemas != null) {
      for (int i = 0; i < childSchemas.size(); i++) {
        List<String> fieldList = null;
        try {
          if (childSchemas.get(i) != null) {
            fieldList = childSchemas.get(i).getFieldNames();
          }
        } catch (NullPointerException ex) {
          LOGGER.info("Field names of given column is null");
        }
        childs[i] = orcToCarbonSchemaConverter(childSchemas.get(i), fieldList,
            fieldsName == null ? null : fieldsName.get(i));
      }
    }
    return childs;
  }

  private static Reader buildOrcReader(String path, Configuration conf) throws IOException {
    try {
      Reader orcReader = OrcFile.createReader(new Path(path),
          OrcFile.readerOptions(conf));
      return orcReader;
    } catch (FileFormatException ex) {
      throw new RuntimeException("File " + path + " is not in ORC format");
    } catch (FileNotFoundException ex) {
      throw new FileNotFoundException("File " + path + " not found to build carbon writer.");
    }
  }

  // Extract the schema of ORC file and convert into carbon schema.
  public static Schema extractOrcFileSchema(CarbonFile dataFile, Configuration conf)
      throws IOException {
    Reader orcReader;
    orcReader = buildOrcReader(dataFile.getPath(), conf);
    TypeDescription typeDescription = orcReader.getSchema();
    List<String> fieldList = null;
    Schema schema;
    try {
      fieldList = typeDescription.getFieldNames();
    } catch (NullPointerException e) {
      LOGGER.info("Field names of given file is null.");
    }
    Field field = orcToCarbonSchemaConverter(typeDescription,
        fieldList, typeDescription.getCategory().getName());
    String fieldType = field.getDataType().toString();
    if (fieldType.equalsIgnoreCase(CarbonCommonConstants.STRUCT)) {
      int size = field.getChildren().size();
      Field[] fields = new Field[size];
      for (int i = 0; i < size; i++) {
        StructField columnDetails = field.getChildren().get(i);
        fields[i] = new Field(columnDetails.getFieldName(),
            columnDetails.getDataType(), columnDetails.getChildren());
      }
      schema = new Schema(fields);
    } else {
      Field[] fields = new Field[1];
      fields[0] = field;
      schema = new Schema(fields);
    }
    return schema;
  }

  // TO convert ORC schema to carbon schema
  private static Field orcToCarbonSchemaConverter(TypeDescription typeDescription,
      List<String> fieldsName, String colName) {
    Objects.requireNonNull(typeDescription, "orc typeDescription should not be null");
    Objects.requireNonNull(typeDescription.getCategory(),
        "typeDescription category should not be null");
    if (colName == null) {
      colName = typeDescription.getCategory().getName();
    }
    switch (typeDescription.getCategory()) {
      case BOOLEAN:
        return new Field(colName, "boolean");
      case BYTE:
      case BINARY:
        return new Field(colName, "binary");
      case SHORT:
        return new Field(colName, "short");
      case INT:
        return new Field(colName, "int");
      case LONG:
        return new Field(colName, "long");
      case FLOAT:
        return new Field(colName, "float");
      case DOUBLE:
        return new Field(colName, "double");
      case DECIMAL:
        return new Field(colName, "decimal");
      case STRING:
        return new Field(colName, "string");
      case CHAR:
      case VARCHAR:
        return new Field(colName, "varchar");
      case DATE:
        return new Field(colName, "date");
      case TIMESTAMP:
        return new Field(colName, "timestamp");
      case STRUCT:
        List<TypeDescription> childSchemas = typeDescription.getChildren();
        Field[] childs = new Field[childSchemas.size()];
        childSchema(childs, childSchemas, fieldsName);
        List<StructField> structList = new ArrayList<>();
        for (int i = 0; i < childSchemas.size(); i++) {
          structList.add(new StructField(childs[i].getFieldName(),
              childs[i].getDataType(), childs[i].getChildren()));
        }
        return new Field(colName, "struct", structList);
      case LIST:
        childSchemas = typeDescription.getChildren();
        childs = new Field[childSchemas.size()];
        childSchema(childs, childSchemas, fieldsName);
        List<StructField> arrayField = new ArrayList<>();
        for (int i = 0; i < childSchemas.size(); i++) {
          arrayField.add(new StructField(childs[i].getFieldName(),
              childs[i].getDataType(), childs[i].getChildren()));
        }
        return new Field(colName, "array", arrayField);
      case MAP:
        childSchemas = typeDescription.getChildren();
        childs = new Field[childSchemas.size()];
        childSchema(childs, childSchemas, fieldsName);
        ArrayList<StructField> keyValueFields = new ArrayList<>();
        StructField keyField = new StructField(typeDescription.getCategory().getName() + ".key",
            childs[0].getDataType());
        StructField valueField = new StructField(typeDescription.getCategory().getName() + ".value",
            childs[1].getDataType(), childs[1].getChildren());
        keyValueFields.add(keyField);
        keyValueFields.add(valueField);
        StructField mapKeyValueField =
            new StructField(typeDescription.getCategory().getName() + ".val",
                DataTypes.createStructType(keyValueFields), keyValueFields);
        MapType mapType =
            DataTypes.createMapType(DataTypes.STRING, mapKeyValueField.getDataType());
        List<StructField> mapStructFields = new ArrayList<>();
        mapStructFields.add(mapKeyValueField);
        return new Field(colName, mapType, mapStructFields);
      default:
        throw new UnsupportedOperationException(
            "carbon not support " + typeDescription.getCategory().getName() + " orc type yet");
    }
  }

  @Override
  public void write(Object object) {
    throw new UnsupportedOperationException("Carbon doesn't support writing a single ORC object");
  }

  @Override
  public void close() throws IOException {
    this.csvCarbonWriter.close();
  }

  /**
   * Load ORC file in iterative way.
   */
  @Override
  public void write() throws IOException {
    if (this.dataFiles == null || this.dataFiles.length == 0) {
      throw new RuntimeException("'withOrcPath()' must be called to support loading ORC files");
    }
    for (CarbonFile dataFile : this.dataFiles) {
      this.loadSingleFile(dataFile);
    }
  }

  private void loadSingleFile(CarbonFile file) throws IOException {
    orcReader = buildOrcReader(file.getPath(), this.configuration);
    ObjectInspector objectInspector = orcReader.getObjectInspector();
    RecordReader recordReader = orcReader.rows();
    Object record = null;
    if (objectInspector instanceof StructObjectInspector) {
      StructObjectInspector structObjectInspector =
          (StructObjectInspector) orcReader.getObjectInspector();
      while (recordReader.hasNext()) {
        record = recordReader.next(record);
        List valueList = structObjectInspector.getStructFieldsDataAsList(record);
        for (int i = 0; i < valueList.size(); i++) {
          valueList.set(i, parseOrcObject(valueList.get(i), 0));
        }
        this.csvCarbonWriter.write(valueList.toArray());
      }
    } else {
      while (recordReader.hasNext()) {
        record = recordReader.next(record);
        this.csvCarbonWriter.write(new Object[]{parseOrcObject(record, 0)});
      }
    }
  }

  private String parseOrcObject(Object recordObject, int level) {
    if (recordObject instanceof OrcStruct) {
      Objects.requireNonNull(orcReader);
      StructObjectInspector structObjectInspector = (StructObjectInspector) orcReader
          .getObjectInspector();
      List value = structObjectInspector.getStructFieldsDataAsList(recordObject);
      for (int i = 0; i < value.size(); i++) {
        value.set(i, parseOrcObject(value.get(i), level + 1));
      }
      String str = listToString(value, level);
      if (str.length() > 0) {
        return str.substring(0, str.length() - 1);
      }
      return null;
    } else if (recordObject instanceof ArrayList) {
      ArrayList listValue = (ArrayList) recordObject;
      for (int i = 0; i < listValue.size(); i++) {
        listValue.set(i, parseOrcObject(listValue.get(i), level + 1));
      }
      String str = listToString(listValue, level);
      if (str.length() > 0) {
        return str.substring(0, str.length() - 1);
      }
      return null;
    } else if (recordObject instanceof LinkedHashMap) {
      LinkedHashMap<Text, Object> keyValueRow = (LinkedHashMap<Text, Object>) recordObject;
      for (Map.Entry<Text, Object> entry : keyValueRow.entrySet()) {
        Object val = parseOrcObject(keyValueRow.get(entry.getKey()), level + 2);
        keyValueRow.put(entry.getKey(), val);
      }
      StringBuilder str = new StringBuilder();
      for (Map.Entry<Text, Object> entry : keyValueRow.entrySet()) {
        Text key = entry.getKey();
        str.append(key.toString()).append("$").append(keyValueRow.get(key)).append("#");
      }
      if (str.length() > 0) {
        return str.substring(0, str.length() - 1);
      }
      return null;
    }
    if (recordObject == null) {
      return null;
    }
    return recordObject.toString();
  }

  private String listToString(List value, int level) {
    String delimiter;
    if (level == 0) {
      delimiter = CarbonCommonConstants.COMPLEX_DELIMITER_LEVEL_1_DEFAULT;
    } else if (level == 1) {
      delimiter = CarbonCommonConstants.COMPLEX_DELIMITER_LEVEL_2_DEFAULT;
    } else if (level == 2) {
      delimiter = CarbonCommonConstants.COMPLEX_DELIMITER_LEVEL_3_DEFAULT;
    } else {
      throw new RuntimeException("carbon only support three level of ORC complex schema");
    }
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < value.size(); i++) {
      str.append(value.get(i)).append(delimiter);
    }
    return str.toString();
  }
}
