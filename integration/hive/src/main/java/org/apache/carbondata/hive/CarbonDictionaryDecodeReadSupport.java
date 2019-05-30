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
import java.util.Calendar;
import java.util.List;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This is the class to decode dictionary encoded column data back to its original value.
 */
public class CarbonDictionaryDecodeReadSupport<T> implements CarbonReadSupport<T> {

  protected Dictionary[] dictionaries;

  protected DataType[] dataTypes;
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
  @Override public void initialize(CarbonColumn[] carbonColumns,
      CarbonTable carbonTable) throws IOException {
    this.carbonColumns = carbonColumns;
    dictionaries = new Dictionary[carbonColumns.length];
    dataTypes = new DataType[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      if (carbonColumns[i].hasEncoding(Encoding.DICTIONARY) && !carbonColumns[i]
          .hasEncoding(Encoding.DIRECT_DICTIONARY) && !carbonColumns[i].isComplex()) {
        CacheProvider cacheProvider = CacheProvider.getInstance();
        Cache<DictionaryColumnUniqueIdentifier, Dictionary> forwardDictionaryCache = cacheProvider
            .createCache(CacheType.FORWARD_DICTIONARY);
        dataTypes[i] = carbonColumns[i].getDataType();
        String dictionaryPath = carbonTable.getTableInfo().getFactTable().getTableProperties()
            .get(CarbonCommonConstants.DICTIONARY_PATH);
        dictionaries[i] = forwardDictionaryCache.get(
            new DictionaryColumnUniqueIdentifier(carbonTable.getAbsoluteTableIdentifier(),
                carbonColumns[i].getColumnIdentifier(), dataTypes[i], dictionaryPath));
      } else {
        dataTypes[i] = carbonColumns[i].getDataType();
      }
    }
  }

  @Override public T readRow(Object[] data) {
    assert (data.length == dictionaries.length);
    writableArr = new Writable[data.length];
    for (int i = 0; i < dictionaries.length; i++) {
      if (dictionaries[i] != null) {
        data[i] = dictionaries[i].getDictionaryValueForKey((int) data[i]);
      }
      try {
        writableArr[i] = createWritableObject(data[i], carbonColumns[i]);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return (T) writableArr;
  }

  /**
   * to book keep the dictionary cache or update access count for each
   * column involved during decode, to facilitate LRU cache policy if memory
   * threshold is reached
   */
  @Override public void close() {
    if (dictionaries == null) {
      return;
    }
    for (int i = 0; i < dictionaries.length; i++) {
      CarbonUtil.clearDictionaryCache(dictionaries[i]);
    }
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
    if (DataTypes.isStructType(dataType)) {
      return createStruct(obj, carbonColumn);
    } else if (DataTypes.isArrayType(dataType)) {
      return createArray(obj, carbonColumn);
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
      ArrayWritable subArray =
          new ArrayWritable(Writable.class, (Writable[]) array.toArray(new Writable[array.size()]));
      return new ArrayWritable(Writable.class, new Writable[] { subArray });
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
    Object[] objArray = (Object[]) obj;
    List<CarbonDimension> childCarbonDimensions = null;
    if (carbonColumn.isDimension() && carbonColumn.getColumnSchema().getNumberOfChild() > 0) {
      childCarbonDimensions = ((CarbonDimension) carbonColumn).getListOfChildDimensions();
    }

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
    if (carbonColumn.hasEncoding(Encoding.DICTIONARY)) {
      obj = DataTypeUtil.getDataBasedOnDataType(obj.toString(), dataType);
      if (obj == null) {
        return null;
      }
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
    } else if (dataType == DataTypes.BOOLEAN) {
      return new BooleanWritable((boolean) obj);
    } else if (dataType == DataTypes.VARCHAR) {
      return new Text(obj.toString());
    } else if (dataType == DataTypes.BINARY) {
      return new BytesWritable((byte[]) obj);
    } else if (dataType == DataTypes.DATE) {
      Calendar c = Calendar.getInstance();
      c.setTime(new Date(0));
      c.add(Calendar.DAY_OF_YEAR, (Integer) obj);
      Date date = new java.sql.Date(c.getTime().getTime());
      return new DateWritable(date);
    } else if (dataType == DataTypes.TIMESTAMP) {
      return new TimestampWritable(new Timestamp((long) obj / 1000));
    } else if (dataType == DataTypes.STRING) {
      return new Text(obj.toString());
    } else if (DataTypes.isArrayType(dataType)) {
      return createArray(obj, carbonColumn);
    } else if (DataTypes.isStructType(dataType)) {
      return createStruct(obj, carbonColumn);
    } else if (DataTypes.isDecimal(dataType)) {
      return new HiveDecimalWritable(HiveDecimal.create(new java.math.BigDecimal(obj.toString())));
    } else {
      throw new IOException("unsupported data type:" + dataType);
    }
  }

}
