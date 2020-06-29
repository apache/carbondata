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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This is the class to convert to Writable object for Hive
 */
public class WritableReadSupport<T> implements CarbonReadSupport<T> {

  /**
   * carbon columns
   */
  protected CarbonColumn[] carbonColumns;

  protected Writable[] writableArr;

  /**
   * This initialization is done inside executor task
   * for column dictionary involved in decoding.
   *
   * @param carbonColumns           column list
   * @param carbonTable table identifier
   */
  @Override
  public void initialize(CarbonColumn[] carbonColumns,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      CarbonTable carbonTable) {
    this.carbonColumns = carbonColumns;
  }

  @Override
  public T readRow(Object[] data) {
    writableArr = new Writable[data.length];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3605
    for (int i = 0; i < data.length; i++) {
      try {
        writableArr[i] = createWritableObject(data[i], carbonColumns[i]);
      } catch (IOException e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
        throw new RuntimeException(e);
      }
    }
    return (T) writableArr;
  }

  /**
   * To Create the Writable from the CarbonData data
   *
   * @param obj
   * @param carbonColumn
   * @return
   * @throws IOException
   */
  private Writable createWritableObject(Object obj, CarbonColumn carbonColumn) throws IOException {
    DataType dataType = carbonColumn.getDataType();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1662
    if (DataTypes.isStructType(dataType)) {
      return createStruct(obj, carbonColumn);
    } else if (DataTypes.isArrayType(dataType)) {
      return createArray(obj, carbonColumn);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3542
    } else if (DataTypes.isMapType(dataType)) {
      return createMap(obj, carbonColumn);
    } else {
      return createWritablePrimitive(obj, carbonColumn);
    }
  }

  /**
   * Create Array Data for Array Datatype
   *
   * @param obj
   * @param carbonColumn
   * @return
   * @throws IOException
   */
  private ArrayWritable createArray(Object obj, CarbonColumn carbonColumn) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3406
    Object[] objArray = (Object[]) obj;
    List<CarbonDimension> childCarbonDimensions = null;
    CarbonDimension arrayDimension = null;
    if (carbonColumn.isDimension() && carbonColumn.getColumnSchema().getNumberOfChild() > 0) {
      childCarbonDimensions = ((CarbonDimension) carbonColumn).getListOfChildDimensions();
      arrayDimension = childCarbonDimensions.get(0);
    }
    List array = new ArrayList();
    if (objArray != null) {
      for (int i = 0; i < objArray.length; i++) {
        Object curObj = objArray[i];
        Writable newObj = createWritableObject(curObj, arrayDimension);
        array.add(newObj);
      }
    }
    if (array.size() > 0) {
      return new ArrayWritable(Writable.class,
              (Writable[]) array.toArray(new Writable[array.size()]));
    }
    return null;
  }

  /**
   * Create the Struct data for the Struct Datatype
   *
   * @param obj
   * @param carbonColumn
   * @return
   * @throws IOException
   */
  private ArrayWritable createStruct(Object obj, CarbonColumn carbonColumn) throws IOException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3406
    Object[] objArray = (Object[]) obj;
    List<CarbonDimension> childCarbonDimensions = null;
    if (carbonColumn.isDimension() && carbonColumn.getColumnSchema().getNumberOfChild() > 0) {
      childCarbonDimensions = ((CarbonDimension) carbonColumn).getListOfChildDimensions();
    }

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2489
    if (null != childCarbonDimensions) {
      Writable[] arr = new Writable[objArray.length];
      for (int i = 0; i < objArray.length; i++) {

        arr[i] = createWritableObject(objArray[i], childCarbonDimensions.get(i));
      }
      return new ArrayWritable(Writable.class, arr);
    }
    return null;
  }

  /**
   * Create the Map data for Map Datatype
   *
   * @param obj
   * @param carbonColumn
   * @return
   * @throws IOException
   */
  private Writable createMap(Object obj, CarbonColumn carbonColumn) throws IOException {
    Object[] objArray = (Object[]) obj;
    List<CarbonDimension> childCarbonDimensions = null;
    CarbonDimension mapDimension = null;
    if (carbonColumn.isDimension() && carbonColumn.getColumnSchema().getNumberOfChild() > 0) {
      childCarbonDimensions = ((CarbonDimension) carbonColumn).getListOfChildDimensions();
      // get the map dimension wrapped inside the carbon dimension
      mapDimension = childCarbonDimensions.get(0);
      // get the child dimenesions of the map dimensions, child dimensions are - Key and Value
      if (null != mapDimension) {
        childCarbonDimensions = mapDimension.getListOfChildDimensions();
      }
    }
    Map<Writable, Writable> rawMap = new HashMap<>();
    if (null != childCarbonDimensions && childCarbonDimensions.size() == 2) {
      Object[] keyObjects = (Object[]) objArray[0];
      Object[] valObjects = (Object[]) objArray[1];
      for (int i = 0; i < keyObjects.length; i++) {
        Writable keyWritable = createWritableObject(keyObjects[i], childCarbonDimensions.get(0));
        Writable valWritable = createWritableObject(valObjects[i], childCarbonDimensions.get(1));
        rawMap.put(keyWritable, valWritable);
      }
    }
    MapWritable mapWritable = new MapWritable();
    mapWritable.putAll(rawMap);
    return mapWritable;
  }

  /**
   * This method will create the Writable Objects for primitives.
   *
   * @param obj
   * @param carbonColumn
   * @return
   * @throws IOException
   */
  private Writable createWritablePrimitive(Object obj, CarbonColumn carbonColumn)
      throws IOException {
    DataType dataType = carbonColumn.getDataType();
    if (obj == null) {
      return null;
    }
    if (dataType == DataTypes.NULL) {
      return null;
    } else if (dataType == DataTypes.DOUBLE) {
      return new DoubleWritable((double) obj);
    } else if (dataType == DataTypes.INT) {
      return new IntWritable((int) obj);
    } else if (dataType == DataTypes.LONG) {
      return new LongWritable((long) obj);
    } else if (dataType == DataTypes.SHORT) {
      return new ShortWritable((short) obj);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3406
    } else if (dataType == DataTypes.BOOLEAN) {
      return new BooleanWritable((boolean) obj);
    } else if (dataType == DataTypes.VARCHAR) {
      return new Text(obj.toString());
    } else if (dataType == DataTypes.BINARY) {
      return new BytesWritable((byte[]) obj);
    } else if (dataType == DataTypes.DATE) {
      return new DateWritableV2((Integer) obj);
    } else if (dataType == DataTypes.TIMESTAMP) {
      WritableTimestampObjectInspector ins = new WritableTimestampObjectInspector();
      return ins.getPrimitiveWritableObject(ins.create(new Timestamp((long) obj / 1000)));
    } else if (dataType == DataTypes.STRING) {
      return new Text(obj.toString());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3406
    } else if (DataTypes.isArrayType(dataType)) {
      return createArray(obj, carbonColumn);
    } else if (DataTypes.isStructType(dataType)) {
      return createStruct(obj, carbonColumn);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3542
    } else if (DataTypes.isMapType(dataType)) {
      return createMap(obj, carbonColumn);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1594
    } else if (DataTypes.isDecimal(dataType)) {
      return new HiveDecimalWritable(HiveDecimal.create(new java.math.BigDecimal(obj.toString())));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3687
    } else if (dataType == DataTypes.FLOAT) {
      return new FloatWritable((float) obj);
    } else {
      throw new IOException("unsupported data type:" + dataType);
    }
  }

}
