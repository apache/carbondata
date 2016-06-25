/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.processing.store.colgroup;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.util.ByteUtil;

/**
 * it gives min max of each column of column group
 */
public class ColGroupMinMax {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ColGroupMinMax.class.getName());

  /**
   * key generator
   */
  private KeyGenerator keyGenerator;

  /**
   * no of column in column group
   */
  private int noOfCol;

  /**
   * min value of each column
   */
  private byte[][] min;

  /**
   * max value of each column
   */
  private byte[][] max;

  /**
   * mask byte range
   */
  private int[][] maskByteRange;

  /**
   * max keys
   */
  private byte[][] maxKeys;

  public ColGroupMinMax(SegmentProperties segmentProperties, int colGroupId) {
    this.keyGenerator = segmentProperties.getColumnGroupAndItsKeygenartor().get(colGroupId);
    this.noOfCol = segmentProperties.getNoOfColumnsInColumnGroup(colGroupId);
    min = new byte[noOfCol][];
    max = new byte[noOfCol][];
    initialise();
  }

  /**
   * @param mdkey
   */
  public void add(byte[] mdkey) {
    for (int i = 0; i < noOfCol; i++) {
      byte[] col = getMaskedKey(mdkey, maskByteRange[i], maxKeys[i]);
      setMin(col, i);
      setMax(col, i);
    }
  }

  /**
   * Below method will be used to get the masked key
   *
   * @param data
   * @return maskedKey
   */
  private byte[] getMaskedKey(byte[] data, int[] maskByteRange, byte[] maxKey) {
    int keySize = maskByteRange.length;
    byte[] maskedKey = new byte[keySize];
    int counter = 0;
    int byteRange = 0;
    for (int i = 0; i < keySize; i++) {
      byteRange = maskByteRange[i];
      maskedKey[counter++] = (byte) (data[byteRange] & maxKey[byteRange]);
    }
    return maskedKey;
  }

  /**
   * intitialising data required for min max calculation
   */
  private void initialise() {
    try {
      maskByteRange = new int[noOfCol][];
      maxKeys = new byte[noOfCol][];
      for (int i = 0; i < noOfCol; i++) {
        maskByteRange[i] = getMaskByteRange(i);
        // generating maxkey
        long[] maxKey = new long[noOfCol];
        maxKey[i] = Long.MAX_VALUE;
        maxKeys[i] = keyGenerator.generateKey(maxKey);
      }
    } catch (KeyGenException e) {
      LOGGER.error(e, "Key generation failed while evaulating column group min max");
    }

  }

  /**
   * get range for given column in generated md key
   *
   * @param col : column
   * @return maskByteRange
   */
  private int[] getMaskByteRange(int col) {
    Set<Integer> integers = new HashSet<>();
    int[] range = keyGenerator.getKeyByteOffsets(col);
    for (int j = range[0]; j <= range[1]; j++) {
      integers.add(j);
    }
    int[] byteIndexs = new int[integers.size()];
    int j = 0;
    for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
      Integer integer = (Integer) iterator.next();
      byteIndexs[j++] = integer.intValue();
    }
    return byteIndexs;
  }

  /**
   * set min value of given column
   *
   * @param colData
   * @param column
   */
  private void setMin(byte[] colData, int column) {

    if (null == min[column]) {
      min[column] = colData;
    } else {
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(colData, min[column]) < 0) {
        min[column] = colData;
      }
    }
  }

  /**
   * set max value of given column
   *
   * @param colData
   * @param column
   */
  private void setMax(byte[] colData, int column) {
    if (null == max[column]) {
      max[column] = colData;
    } else {
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(colData, max[column]) > 0) {
        max[column] = colData;
      }

    }
  }

  /**
   * Get min value of block
   *
   * @return min value of block
   */
  public byte[] getMin() {
    int size = 0;
    for (int i = 0; i < noOfCol; i++) {
      size += min[i].length;
    }
    ByteBuffer bb = ByteBuffer.allocate(size);
    for (int i = 0; i < noOfCol; i++) {
      bb.put(min[i]);
    }
    return bb.array();
  }

  /**
   * get max value of block
   *
   * @return max value of block
   */
  public byte[] getMax() {
    int size = 0;
    for (int i = 0; i < noOfCol; i++) {
      size += max[i].length;
    }
    ByteBuffer bb = ByteBuffer.allocate(size);
    for (int i = 0; i < noOfCol; i++) {
      bb.put(max[i]);
    }
    return bb.array();
  }

}
