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

package org.carbondata.core.datastorage.store.impl.data.uncompressed;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.MeasureDataWrapper;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.carbondata.core.datastorage.store.impl.CompressedDataMeasureDataWrapper;

public class DoubleArrayDataFileStore extends AbstractDoubleArrayDataStore {

  private long[] measuresOffsetsArray;

  private int[] measuresLengthArray;

  private String fileName;

  public DoubleArrayDataFileStore(ValueCompressionModel compressionModel,
      long[] measuresOffsetsArray, String fileName, int[] measuresLengthArray) {
    super(compressionModel);
    this.fileName = fileName;
    this.measuresLengthArray = measuresLengthArray;
    this.measuresOffsetsArray = measuresOffsetsArray;
  }

  @Override public MeasureDataWrapper getBackData(int[] cols, FileHolder fileHolder) {
    if (null == compressionModel) {
      return null;
    }
    UnCompressValue[] unComp = new UnCompressValue[measuresLengthArray.length];
    CarbonReadDataHolder[] vals = new CarbonReadDataHolder[measuresLengthArray.length];
    if (cols != null) {
      for (int i = 0; i < cols.length; i++) {
        unComp[cols[i]] = compressionModel.getUnCompressValues()[cols[i]].getNew();
        unComp[cols[i]].setValueInBytes(fileHolder
            .readByteArray(fileName, measuresOffsetsArray[cols[i]], measuresLengthArray[cols[i]]));
        vals[cols[i]] = unComp[cols[i]].getValues(compressionModel.getDecimal()[cols[i]],
            compressionModel.getMaxValue()[cols[i]]);
      }
    } else {
      for (int i = 0; i < unComp.length; i++) {

        unComp[i] = compressionModel.getUnCompressValues()[i].getNew();
        unComp[i].setValueInBytes(
            fileHolder.readByteArray(fileName, measuresOffsetsArray[i], measuresLengthArray[i]));
        vals[i] = unComp[i]
            .getValues(compressionModel.getDecimal()[i], compressionModel.getMaxValue()[i]);
      }
    }
    return new CompressedDataMeasureDataWrapper(vals);
  }

  @Override public MeasureDataWrapper getBackData(int cols, FileHolder fileHolder) {
    if (null == compressionModel) {
      return null;
    }
    UnCompressValue[] unComp = new UnCompressValue[measuresLengthArray.length];
    CarbonReadDataHolder[] vals = new CarbonReadDataHolder[measuresLengthArray.length];

    unComp[cols] = compressionModel.getUnCompressValues()[cols].getNew();
    unComp[cols].setValueInBytes(
        fileHolder.readByteArray(fileName, measuresOffsetsArray[cols], measuresLengthArray[cols]));
    vals[cols] = unComp[cols]
        .getValues(compressionModel.getDecimal()[cols], compressionModel.getMaxValue()[cols]);
    return new CompressedDataMeasureDataWrapper(vals);
  }
}
