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
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

class CarbonHiveRecordReader extends CarbonRecordReader<ArrayWritable>
    implements org.apache.hadoop.mapred.RecordReader<Void, ArrayWritable> {

  private ArrayWritable valueObj = null;
  private long recordReaderCounter = 0;
  private int[] columnIds;

  public CarbonHiveRecordReader(QueryModel queryModel, CarbonReadSupport<ArrayWritable> readSupport,
      InputSplit inputSplit, JobConf jobConf) throws IOException {
    super(queryModel, readSupport, jobConf);
    initialize(inputSplit, jobConf);
  }

  private void initialize(InputSplit inputSplit, Configuration conf) throws IOException {
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
    readSupport
        .initialize(queryModel.getProjectionColumns(), queryModel.getTable());
    try {
      carbonIterator = new ChunkRowIterator(queryExecutor.execute(queryModel));
    } catch (QueryExecutionException e) {
      throw new IOException(e.getMessage(), e.getCause());
    }
    final TypeInfo rowTypeInfo;
    final List<String> columnNames;
    List<TypeInfo> columnTypes;
    // Get column names and sort order
    final String colIds = conf.get("hive.io.file.readcolumn.ids");
    final String columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);

    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    if (valueObj == null) {
      valueObj = new ArrayWritable(Writable.class, new Writable[columnTypes.size()]);
    }

    if (null != colIds && !colIds.equals("")) {
      String[] arraySelectedColId = colIds.split(",");
      columnIds = new int[arraySelectedColId.length];
      int columnId = 0;
      for (int j = 0; j < arraySelectedColId.length; j++) {
        columnId = Integer.parseInt(arraySelectedColId[j]);
        columnIds[j] = columnId;
      }
    }

  }

  @Override public boolean next(Void aVoid, ArrayWritable value) throws IOException {
    if (carbonIterator.hasNext()) {
      Object obj = readSupport.readRow(carbonIterator.next());
      recordReaderCounter++;
      Writable[] objArray = (Writable[]) obj;
      Writable[] sysArray = new Writable[value.get().length];
      if (columnIds != null && columnIds.length > 0 && objArray.length == columnIds.length) {
        for (int i = 0; i < columnIds.length; i++) {
          sysArray[columnIds[i]] = objArray[i];
        }
        value.set(sysArray);
      } else {
        value.set(objArray);
      }
      return true;
    } else {
      return false;
    }
  }

  @Override public Void createKey() {
    return null;
  }

  @Override public ArrayWritable createValue() {
    return valueObj;
  }

  @Override public long getPos() throws IOException {
    return recordReaderCounter;
  }

  @Override public float getProgress() throws IOException {
    return 0;
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
        return new DoubleWritable((double) obj);
      case INT:
        return new IntWritable((int) obj);
      case LONG:
        return new LongWritable((long) obj);
      case SHORT:
        return new ShortWritable((short) obj);
      case BOOLEAN:
        return new BooleanWritable((boolean) obj);
      case VARCHAR:
        return new Text(obj.toString());
      case BINARY:
        return new BytesWritable((byte[]) obj);
      case DATE:
        return new DateWritable(new Date(Long.parseLong(String.valueOf(obj.toString()))));
      case TIMESTAMP:
        return new TimestampWritable(new Timestamp((long) obj));
      case STRING:
        return new Text(obj.toString());
      case CHAR:
        return new Text(obj.toString());
      case DECIMAL:
        return new HiveDecimalWritable(
            HiveDecimal.create((java.math.BigDecimal) obj));
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