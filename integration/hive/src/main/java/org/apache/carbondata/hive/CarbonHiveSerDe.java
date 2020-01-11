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

package org.apache.carbondata.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.parquet.serde.ArrayWritableObjectInspector;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * A serde class for Carbondata.
 * It transparently passes the object to/from the Carbon file reader/writer.
 */
@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES })
public class CarbonHiveSerDe extends AbstractSerDe {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonHiveSerDe.class.getCanonicalName());

  private final SerDeStats stats;
  private ObjectInspector objInspector;

  private enum LAST_OPERATION {
    SERIALIZE, DESERIALIZE, UNKNOWN
  }

  private LAST_OPERATION status;
  private long serializedSize;
  private long deserializedSize;

  public CarbonHiveSerDe() {
    stats = new SerDeStats();
  }

  @Override
  public void initialize(@Nullable Configuration configuration, Properties tbl)
      throws SerDeException {
    final TypeInfo rowTypeInfo;
    final List<String> columnNames;
    final List<TypeInfo> columnTypes;
    // Get column names and sort order
    assert configuration != null;
    final String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }
    inferSchema(tbl, columnNames, columnTypes);

    // Create row related objects
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    this.objInspector = new ArrayWritableObjectInspector((StructTypeInfo) rowTypeInfo);
    // Stats part
    serializedSize = 0;
    deserializedSize = 0;
    status = LAST_OPERATION.UNKNOWN;
  }

  private void inferSchema(Properties tbl, List<String> columnNames, List<TypeInfo> columnTypes) {
    if (columnNames.size() == 0 && columnTypes.size() == 0) {
      String external = tbl.getProperty("EXTERNAL");
      String location = tbl.getProperty(hive_metastoreConstants.META_TABLE_LOCATION);
      if (external != null && "TRUE".equals(external) && location != null) {
        String[] names =
            tbl.getProperty(hive_metastoreConstants.META_TABLE_NAME).split("\\.");
        if (names.length == 2) {
          AbsoluteTableIdentifier identifier =
              AbsoluteTableIdentifier.from(location, names[0], names[1]);
          String schemaPath = CarbonTablePath.getSchemaFilePath(identifier.getTablePath());
          try {
            TableInfo tableInfo = null;
            if (!FileFactory.isFileExist(schemaPath)) {
              tableInfo = SchemaReader.inferSchema(identifier, false);
            } else {
              tableInfo = SchemaReader.getTableInfo(identifier);
            }
            if (tableInfo != null) {
              CarbonTable carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
              List<CarbonColumn> columns = carbonTable.getCreateOrderColumn();
              for (CarbonColumn column : columns) {
                columnNames.add(column.getColName());
                columnTypes.add(HiveDataTypeUtils.convertCarbonDataTypeToHive(column));
              }
            }
          } catch (Exception ex) {
            LOGGER.warn("Failed to infer schema: " + ex.getMessage());
          }
        }
      }
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return ArrayWritable.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objectInspector)
      throws SerDeException {
    if (!objInspector.getCategory().equals(ObjectInspector.Category.STRUCT)) {
      throw new SerDeException("Cannot serializeStartKey " + objInspector.getCategory()
          + ". Can only serializeStartKey a struct");
    }
    serializedSize += ((StructObjectInspector) objInspector).getAllStructFieldRefs().size();
    status = LAST_OPERATION.SERIALIZE;
    return createCarbonRow(obj, (StructObjectInspector) objectInspector);
  }

  private CarbonHiveRow createCarbonRow(Object obj, StructObjectInspector inspector)
      throws SerDeException {
    List fields = inspector.getAllStructFieldRefs();
    CarbonHiveRow carbonHiveRow = new CarbonHiveRow();
    for (int i = 0; i < fields.size(); i++) {
      StructField field = (StructField) fields.get(i);
      Object subObj = inspector.getStructFieldData(obj, field);
      ObjectInspector subInspector = field.getFieldObjectInspector();
      carbonHiveRow.addToRow(createObject(subObj, subInspector));
    }
    return carbonHiveRow;
  }

  private Object[] createStruct(Object obj, StructObjectInspector inspector)
      throws SerDeException {
    List fields = inspector.getAllStructFieldRefs();
    Object[] arr = new Object[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      StructField field = (StructField) fields.get(i);
      Object subObj = inspector.getStructFieldData(obj, field);
      ObjectInspector subInspector = field.getFieldObjectInspector();
      arr[i] = createObject(subObj, subInspector);
    }
    return arr;
  }

  private Object[] createArray(Object obj, ListObjectInspector inspector)
      throws SerDeException {
    List sourceArray = inspector.getList(obj);
    ObjectInspector subInspector = inspector.getListElementObjectInspector();
    List array = new ArrayList();
    Iterator iterator;
    if (sourceArray != null) {
      for (iterator = sourceArray.iterator(); iterator.hasNext(); ) {
        Object curObj = iterator.next();
        Object newObj = createObject(curObj, subInspector);
        if (newObj != null) {
          array.add(newObj);
        }
      }
    }
    return array.toArray();
  }

  private Object createPrimitive(Object obj, PrimitiveObjectInspector inspector)
      throws SerDeException {
    if (obj == null) {
      return null;
    }
    return inspector.getPrimitiveWritableObject(obj).toString();
  }

  private Object createObject(Object obj, ObjectInspector inspector) throws SerDeException {
    switch (inspector.getCategory()) {
      case STRUCT:
        return createStruct(obj, (StructObjectInspector) inspector);
      case LIST:
        return createArray(obj, (ListObjectInspector) inspector);
      case PRIMITIVE:
        return createPrimitive(obj, (PrimitiveObjectInspector) inspector);
      case MAP:
        return createMap(obj, (MapObjectInspector) inspector);
    }
    throw new SerDeException("Unknown data type" + inspector.getCategory());
  }

  private Object[] createMap(Object obj, MapObjectInspector inspector) throws SerDeException {
    Map<Writable, Writable> map = (Map<Writable, Writable>) inspector.getMap(obj);
    Object[] result = new Object[map.size()];
    int i = 0;
    for (Map.Entry<Writable, Writable> entry : map.entrySet()) {
      result[i++] =
          new Object[] { createObject(entry.getKey(), inspector.getMapKeyObjectInspector()),
              createObject(entry.getValue(), inspector.getMapValueObjectInspector()) };
    }
    return result;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // must be different
    assert (status != LAST_OPERATION.UNKNOWN);
    if (status == LAST_OPERATION.SERIALIZE) {
      stats.setRawDataSize(serializedSize);
    } else {
      stats.setRawDataSize(deserializedSize);
    }
    return stats;
  }

  @Override
  public Object deserialize(Writable writable) {
    status = LAST_OPERATION.DESERIALIZE;
    if (writable instanceof ArrayWritable) {
      deserializedSize += ((StructObjectInspector) objInspector).getAllStructFieldRefs().size();
      return writable;
    } else {
      return null;
    }
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return objInspector;
  }
}