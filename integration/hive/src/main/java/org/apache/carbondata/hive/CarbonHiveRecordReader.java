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


import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.iterator.ChunkRowIterator;
import org.apache.carbondata.hadoop.CarbonRecordReader;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

public class CarbonHiveRecordReader extends CarbonRecordReader<ArrayWritable>
    implements org.apache.hadoop.mapred.RecordReader<Void, ArrayWritable> {

  ArrayWritable valueObj = null;
  private CarbonObjectInspector objInspector;

  public CarbonHiveRecordReader(QueryModel queryModel, CarbonReadSupport<ArrayWritable> readSupport,
                                InputSplit inputSplit, JobConf jobConf) throws IOException {
    super(queryModel, readSupport);
    initialize(inputSplit, jobConf);
  }

  public void initialize(InputSplit inputSplit, Configuration conf) throws IOException {
    // The input split can contain single HDFS block or multiple blocks, so firstly get all the
    // blocks and then set them in the query model.
    List<CarbonHiveInputSplit> splitList;
    if (inputSplit instanceof CarbonHiveInputSplit) {
      splitList = new ArrayList<>(1);
      splitList.add((CarbonHiveInputSplit) inputSplit);
    } else {
      throw new RuntimeException("unsupported input split type: " + inputSplit);
    }
    List<TableBlockInfo> tableBlockInfoList = CarbonHiveInputSplit.createBlocks(splitList);
    queryModel.setTableBlockInfos(tableBlockInfoList);
    readSupport.initialize(queryModel.getProjectionColumns(),
        queryModel.getAbsoluteTableIdentifier());
    try {
      carbonIterator = new ChunkRowIterator(queryExecutor.execute(queryModel));
    } catch (QueryExecutionException e) {
      throw new IOException(e.getMessage(), e.getCause());
    }
    if (valueObj == null) {
      valueObj = new ArrayWritable(Writable.class,
          new Writable[queryModel.getProjectionColumns().length]);
    }

    final TypeInfo rowTypeInfo;
    final List<String> columnNames;
    List<TypeInfo> columnTypes;
    // Get column names and sort order
    final String columnNameProperty = conf.get("hive.io.file.readcolumn.names");
    final String columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);

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
    columnTypes = columnTypes.subList(0, columnNames.size());
    // Create row related objects
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    this.objInspector = new CarbonObjectInspector((StructTypeInfo) rowTypeInfo);
  }

  @Override
  public boolean next(Void aVoid, ArrayWritable value) throws IOException {
    if (carbonIterator.hasNext()) {
      Object obj = readSupport.readRow(carbonIterator.next());
      ArrayWritable tmpValue = null;
      try {
        tmpValue = createArrayWritable(obj);
      } catch (SerDeException se) {
        throw new IOException(se.getMessage(), se.getCause());
      }

      if (valueObj != tmpValue) {
        final Writable[] arrValue = value.get();
        final Writable[] arrCurrent = tmpValue.get();
        if (valueObj != null && arrValue.length == arrCurrent.length) {
          System.arraycopy(arrCurrent, 0, arrValue, 0, arrCurrent.length);
        } else {
          if (arrValue.length != arrCurrent.length) {
            throw new IOException("CarbonHiveInput : size of object differs. Value" +
              " size :  " + arrValue.length + ", Current Object size : " + arrCurrent.length);
          } else {
            throw new IOException("CarbonHiveInput can not support RecordReaders that" +
              " don't return same key & value & value is null");
          }
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public ArrayWritable createArrayWritable(Object obj) throws SerDeException {
    return createStruct(obj, objInspector);
  }

  @Override
  public Void createKey() {
    return null;
  }

  @Override
  public ArrayWritable createValue() {
    return valueObj;
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }

  public ArrayWritable createStruct(Object obj, StructObjectInspector inspector)
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

      return new ArrayWritable(Writable.class, new Writable[]{subArray});
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
        return new DoubleWritable((double) obj);
      case INT:
        return new IntWritable(((Long) obj).intValue());
      case LONG:
        return new LongWritable((long) obj);
      case SHORT:
        return new ShortWritable((short) obj);
      case DATE:
        return new DateWritable(new Date(((long) obj)));
      case TIMESTAMP:
        return new TimestampWritable(new Timestamp((long) obj));
      case STRING:
        return new Text(obj.toString());
      case DECIMAL:
        return new HiveDecimalWritable(HiveDecimal.create(
          ((org.apache.spark.sql.types.Decimal) obj).toJavaBigDecimal()));
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
}
