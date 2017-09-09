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
import java.util.List;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.GenericArrayData;

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
   * @param absoluteTableIdentifier table identifier
   */
  @Override public void initialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws IOException {
    this.carbonColumns = carbonColumns;
    dictionaries = new Dictionary[carbonColumns.length];
    dataTypes = new DataType[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      if (carbonColumns[i].hasEncoding(Encoding.DICTIONARY) && !carbonColumns[i]
          .hasEncoding(Encoding.DIRECT_DICTIONARY) && !carbonColumns[i].isComplex()) {
        CacheProvider cacheProvider = CacheProvider.getInstance();
        Cache<DictionaryColumnUniqueIdentifier, Dictionary> forwardDictionaryCache = cacheProvider
            .createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier.getStorePath());
        dataTypes[i] = carbonColumns[i].getDataType();
        dictionaries[i] = forwardDictionaryCache.get(
            new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier.getCarbonTableIdentifier(),
                carbonColumns[i].getColumnIdentifier(), dataTypes[i],
                CarbonStorePath.getCarbonTablePath(absoluteTableIdentifier)));
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
        throw new RuntimeException(e.getMessage(), e);
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
    switch (dataType) {
      case STRUCT:
        return createStruct(obj, carbonColumn);
      case ARRAY:
        return createArray(obj, carbonColumn);
      default:
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
    if (obj instanceof GenericArrayData) {
      Object[] objArray = ((GenericArrayData) obj).array();
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
        ArrayWritable subArray = new ArrayWritable(Writable.class,
            (Writable[]) array.toArray(new Writable[array.size()]));
        return new ArrayWritable(Writable.class, new Writable[] { subArray });
      }
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
    if (obj instanceof GenericInternalRow) {
      Object[] objArray = ((GenericInternalRow) obj).values();
      List<CarbonDimension> childCarbonDimensions = null;
      if (carbonColumn.isDimension() && carbonColumn.getColumnSchema().getNumberOfChild() > 0) {
        childCarbonDimensions = ((CarbonDimension) carbonColumn).getListOfChildDimensions();
      }
      Writable[] arr = new Writable[objArray.length];
      for (int i = 0; i < objArray.length; i++) {

        arr[i] = createWritableObject(objArray[i], childCarbonDimensions.get(i));
      }
      return new ArrayWritable(Writable.class, arr);
    }
    throw new IOException("DataType not supported in Carbondata");
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
    switch (dataType) {
      case NULL:
        return null;
      case DOUBLE:
        return new DoubleWritable((double) obj);
      case INT:
        return new IntWritable((int) obj);
      case LONG:
        return new LongWritable((long) obj);
      case SHORT:
        return new ShortWritable((Short) obj);
      case DATE:
        return new DateWritable(new Date(((Integer) obj).longValue()));
      case TIMESTAMP:
        return new TimestampWritable(new Timestamp((long) obj / 1000));
      case STRING:
        return new Text(obj.toString());
      case DECIMAL:
        return new HiveDecimalWritable(
            HiveDecimal.create(new java.math.BigDecimal(obj.toString())));
      default:
        throw new IOException("unsupported data type:" + dataType);
    }
  }

  /**
   * If we need to use the same Writable[] then we can use this method
   *
   * @param writable
   * @param obj
   * @param carbonColumn
   * @throws IOException
   */
  private void setPrimitive(Writable writable, Object obj, CarbonColumn carbonColumn)
      throws IOException {
    DataType dataType = carbonColumn.getDataType();
    if (obj == null) {
      writable.write(null);
    }
    switch (dataType) {
      case DOUBLE:
        ((DoubleWritable) writable).set((double) obj);
        break;
      case INT:
        ((IntWritable) writable).set((int) obj);
        break;
      case LONG:
        ((LongWritable) writable).set((long) obj);
        break;
      case SHORT:
        ((ShortWritable) writable).set((short) obj);
        break;
      case DATE:
        ((DateWritable) writable).set(new Date((Long) obj));
        break;
      case TIMESTAMP:
        ((TimestampWritable) writable).set(new Timestamp((long) obj));
        break;
      case STRING:
        ((Text) writable).set(obj.toString());
        break;
      case DECIMAL:
        ((HiveDecimalWritable) writable)
            .set(HiveDecimal.create(new java.math.BigDecimal(obj.toString())));
        break;
      default:
        throw new IOException("unsupported data type:" + dataType);
    }
  }

}
