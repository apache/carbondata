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
import java.util.Properties;
import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 * A serde class for Carbondata.
 * It transparently passes the object to/from the Carbon file reader/writer.
 */
@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES })
class CarbonHiveSerDe extends AbstractSerDe {
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

  @Override public void initialize(@Nullable Configuration configuration, Properties tbl)
      throws SerDeException {

    final TypeInfo rowTypeInfo;
    final List<String> columnNames;
    final List<String> reqColNames;
    final List<TypeInfo> columnTypes;
    // Get column names and sort order
    assert configuration != null;
    final String colIds = configuration.get("hive.io.file.readcolumn.ids");

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
    if (colIds != null && !colIds.equals("")) {
      reqColNames = new ArrayList<String>();

      String[] arraySelectedColId = colIds.split(",");
      List<TypeInfo> reqColTypes = new ArrayList<TypeInfo>();
      for (String anArrayColId : arraySelectedColId) {
        reqColNames.add(columnNames.get(Integer.parseInt(anArrayColId)));
        reqColTypes.add(columnTypes.get(Integer.parseInt(anArrayColId)));
      }
      // Create row related objects
      rowTypeInfo = TypeInfoFactory.getStructTypeInfo(reqColNames, reqColTypes);
      this.objInspector = new CarbonObjectInspector((StructTypeInfo) rowTypeInfo);
    }
    else {
      // Create row related objects
      rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
      this.objInspector = new CarbonObjectInspector((StructTypeInfo) rowTypeInfo);

      // Stats part
      serializedSize = 0;
      deserializedSize = 0;
      status = LAST_OPERATION.UNKNOWN;
    }
  }

  @Override public Class<? extends Writable> getSerializedClass() {
    return ArrayWritable.class;
  }

  @Override public Writable serialize(Object obj, ObjectInspector objectInspector)
      throws SerDeException {
    if (!objInspector.getCategory().equals(ObjectInspector.Category.STRUCT)) {
      throw new SerDeException("Cannot serializeStartKey " + objInspector.getCategory()
          + ". Can only serializeStartKey a struct");
    }
    serializedSize += ((StructObjectInspector) objInspector).getAllStructFieldRefs().size();
    status = LAST_OPERATION.SERIALIZE;
    return createStruct(obj, (StructObjectInspector) objInspector);
  }

  private ArrayWritable createStruct(Object obj, StructObjectInspector inspector)
      throws SerDeException {
    List fields = inspector.getAllStructFieldRefs();
    Writable[] arr = new Writable[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      StructField field = (StructField) fields.get(i);
      Object subObj = inspector.getStructFieldData(obj, field);
      ObjectInspector subInspector = field.getFieldObjectInspector();
      arr[i] = createObject(subObj, subInspector);
    }
    return new ArrayWritable(Writable.class, arr);
  }

  private ArrayWritable createArray(Object obj, ListObjectInspector inspector)
      throws SerDeException {
    List sourceArray = inspector.getList(obj);
    ObjectInspector subInspector = inspector.getListElementObjectInspector();
    List array = new ArrayList();
    Iterator iterator;
    if (sourceArray != null) {
      for (iterator = sourceArray.iterator(); iterator.hasNext(); ) {
        Object curObj = iterator.next();
        Writable newObj = createObject(curObj, subInspector);
        if (newObj != null) {
          array.add(newObj);
        }
      }
    }
    if (array.size() > 0) {
      ArrayWritable subArray = new ArrayWritable(((Writable) array.get(0)).getClass(),
          (Writable[]) array.toArray(new Writable[array.size()]));

      return new ArrayWritable(Writable.class, new Writable[] { subArray });
    }
    return null;
  }

  private Writable createPrimitive(Object obj, PrimitiveObjectInspector inspector)
      throws SerDeException {
    if (obj == null) {
      return null;
    }
    switch (inspector.getPrimitiveCategory()) {
      case VOID:
        return null;
      case DOUBLE:
        return new DoubleWritable(((DoubleObjectInspector) inspector).get(obj));
      case INT:
        return new IntWritable(((IntObjectInspector) inspector).get(obj));
      case LONG:
        return new LongWritable(((LongObjectInspector) inspector).get(obj));
      case SHORT:
        return new ShortWritable(((ShortObjectInspector) inspector).get(obj));
      case TIMESTAMP:
        return ((TimestampObjectInspector) inspector).getPrimitiveWritableObject(obj);
      case DATE:
        return ((DateObjectInspector) inspector).getPrimitiveWritableObject(obj);
      case STRING:
        return ((StringObjectInspector) inspector).getPrimitiveWritableObject(obj);
      case CHAR:
        return ((StringObjectInspector) inspector).getPrimitiveWritableObject(obj);
      case DECIMAL:
        return ((HiveDecimalObjectInspector) inspector).getPrimitiveWritableObject(obj);
    }
    throw new SerDeException("Unknown primitive : " + inspector.getPrimitiveCategory());
  }

  private Writable createObject(Object obj, ObjectInspector inspector) throws SerDeException {
    switch (inspector.getCategory()) {
      case STRUCT:
        return createStruct(obj, (StructObjectInspector) inspector);
      case LIST:
        return createArray(obj, (ListObjectInspector) inspector);
      case PRIMITIVE:
        return createPrimitive(obj, (PrimitiveObjectInspector) inspector);
    }
    throw new SerDeException("Unknown data type" + inspector.getCategory());
  }

  @Override public SerDeStats getSerDeStats() {
    // must be different
    assert (status != LAST_OPERATION.UNKNOWN);
    if (status == LAST_OPERATION.SERIALIZE) {
      stats.setRawDataSize(serializedSize);
    } else {
      stats.setRawDataSize(deserializedSize);
    }
    return stats;
  }

  @Override public Object deserialize(Writable writable) throws SerDeException {
    status = LAST_OPERATION.DESERIALIZE;
    if (writable instanceof ArrayWritable) {
      deserializedSize += ((StructObjectInspector) objInspector).getAllStructFieldRefs().size();
      return writable;
    } else {
      return null;
    }
  }

  @Override public ObjectInspector getObjectInspector() throws SerDeException {
    return objInspector;
  }
}