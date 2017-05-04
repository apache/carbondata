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
package org.apache.carbondata.core.util;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ValueCompressionUtil.COMPRESSION_TYPE;


/**
 * through the size of data type,priority and compression type, select the
 * best compression type
 */
public class CompressionFinder implements Comparable<CompressionFinder> {
  private COMPRESSION_TYPE compType;

  private DataType actualDataType;

  private DataType convertedDataType;
  /**
   * the size of changed data
   */
  private int size;

  private PRIORITY priority;

  private DataType measureStoreType;

  /**
   * CompressionFinder constructor.
   *
   * @param compType
   * @param actualDataType
   * @param convertedDataType
   */
  CompressionFinder(COMPRESSION_TYPE compType, DataType actualDataType,
      DataType convertedDataType, DataType measureStoreType) {
    super();
    this.compType = compType;
    this.actualDataType = actualDataType;
    this.convertedDataType = convertedDataType;
    this.measureStoreType = measureStoreType;
  }

  /**
   * CompressionFinder overloaded constructor.
   *
   * @param compType
   * @param actualDataType
   * @param convertedDataType
   * @param priority
   */

  CompressionFinder(COMPRESSION_TYPE compType, DataType actualDataType, DataType convertedDataType,
      PRIORITY priority, DataType measureStoreType) {
    super();
    this.actualDataType = actualDataType;
    this.convertedDataType = convertedDataType;
    this.size = ValueCompressionUtil.getSize(convertedDataType);
    this.priority = priority;
    this.compType = compType;
    this.measureStoreType = measureStoreType;
  }

  @Override public boolean equals(Object obj) {
    boolean equals = false;
    if (obj instanceof CompressionFinder) {
      CompressionFinder cf = (CompressionFinder) obj;

      if (this.size == cf.size && this.priority == cf.priority) {
        equals = true;
      }

    }
    return equals;
  }

  @Override public int hashCode() {
    final int code = 31;
    int result = 1;

    result = code * result + this.size;
    result = code * result + ((priority == null) ? 0 : priority.hashCode());
    return result;
  }

  @Override public int compareTo(CompressionFinder o) {
    int returnVal = 0;
    // the big size have high priority
    if (this.equals(o)) {
      returnVal = 0;
    } else if (this.size == o.size) {
      // the compression type priority
      if (priority.priority > o.priority.priority) {
        returnVal = 1;
      } else if (priority.priority < o.priority.priority) {
        returnVal = -1;
      }

    } else if (this.size > o.size) {
      returnVal = 1;
    } else {
      returnVal = -1;
    }
    return returnVal;
  }

  /**
   * Compression type priority.
   * ACTUAL is the highest priority and DIFFNONDECIMAL is the lowest
   * priority
   */
  enum PRIORITY {
    ACTUAL(0),
    DIFFSIZE(1),
    MAXNONDECIMAL(2),
    DIFFNONDECIMAL(3);
    private int priority;

    PRIORITY(int priority) {
      this.priority = priority;
    }
  }

  public COMPRESSION_TYPE getCompType() {
    return compType;
  }

  public DataType getActualDataType() {
    return actualDataType;
  }

  public DataType getConvertedDataType() {
    return convertedDataType;
  }

  public int getSize() {
    return size;
  }

  public PRIORITY getPriority() {
    return priority;
  }

  public DataType getMeasureStoreType() {
    return measureStoreType;
  }
}
