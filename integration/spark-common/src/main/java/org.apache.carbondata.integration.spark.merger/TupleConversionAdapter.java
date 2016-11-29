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
package org.apache.carbondata.integration.spark.merger;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.processing.util.RemoveDictionaryUtil;
import org.apache.carbondata.scan.wrappers.ByteArrayWrapper;

/**
 * This class will be used to convert the Result into the format used in data writer.
 */
public class TupleConversionAdapter {

  private final SegmentProperties segmentproperties;

  private final List<CarbonMeasure> measureList;

  private int noDictionaryPresentIndex;

  private int measureCount;

  private boolean isNoDictionaryPresent;

  public TupleConversionAdapter(SegmentProperties segmentProperties) {
    this.measureCount = segmentProperties.getMeasures().size();
    this.isNoDictionaryPresent = segmentProperties.getNumberOfNoDictionaryDimension() > 0;
    if (isNoDictionaryPresent) {
      noDictionaryPresentIndex++;
    }
    this.segmentproperties = segmentProperties;
    measureList = segmentProperties.getMeasures();
  }

  /**
   * Converting the raw result to the format understandable by the data writer.
   * @param carbonTuple
   * @return
   */
  public Object[] getObjectArray(Object[] carbonTuple) {
    Object[] row = new Object[measureCount + noDictionaryPresentIndex + 1];
    int index = 0;
    // put measures.

    for (int j = 1; j <= measureCount; j++) {
      row[index++] = carbonTuple[j];
    }

    // put No dictionary byte []
    if (isNoDictionaryPresent) {

      int noDicCount = segmentproperties.getNumberOfNoDictionaryDimension();
      List<byte[]> noDicByteArr = new ArrayList<>(noDicCount);
      for (int i = 0; i < noDicCount; i++) {
        noDicByteArr.add(((ByteArrayWrapper) carbonTuple[0]).getNoDictionaryKeyByIndex(i));
      }
      byte[] singleByteArr = RemoveDictionaryUtil.convertListByteArrToSingleArr(noDicByteArr);

      row[index++] = singleByteArr;
    }

    // put No Dictionary Dims
    row[index++] = ((ByteArrayWrapper) carbonTuple[0]).getDictionaryKey();
    return row;
  }
}
