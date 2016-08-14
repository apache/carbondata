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

package org.apache.carbondata.core.datastorage.store.impl.data.uncompressed;

import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.MeasureDataWrapper;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.datastorage.store.impl.CompressedDataMeasureDataWrapper;

/**
 * DoubleArrayDataInMemoryStore.
 *
 * @author S71955
 */
public class DoubleArrayDataInMemoryStore extends AbstractDoubleArrayDataStore {

  // /**
  // * DoubleArrayDataInMemoryStore.
  // * @param size
  // * @param elementSize
  // * @param compressionModel
  // */
  // public DoubleArrayDataInMemoryStore(int size, int elementSize,
  // ValueCompressionModel compressionModel)
  // {
  // super(size, elementSize, compressionModel);
  // }
  //
  // /**
  // * DoubleArrayDataInMemoryStore.
  // * @param size
  // * @param elementSize
  // */
  // public DoubleArrayDataInMemoryStore(int size, int elementSize)
  // {
  // super(size, elementSize);
  // }

  // /**
  // * DoubleArrayDataInMemoryStore.
  // * @param size
  // * @param elementSize
  // * @param compressionModel
  // * @param measuresOffsetsArray
  // * @param measuresLengthArray
  // * @param fileName
  // * @param fileHolder
  // */
  // public DoubleArrayDataInMemoryStore(int size, int elementSize,
  // ValueCompressionModel compressionModel,
  // long[] measuresOffsetsArray, int[] measuresLengthArray, String fileName,
  // FileHolder fileHolder)
  // {
  // super(size, elementSize, compressionModel);
  // UnCompressValue[] unCompValues = compressionModel.getUnCompressValues();
  // if(null != unCompValues)
  // {
  // for(int i = 0;i < measuresLengthArray.length;i++)
  // {
  //
  // values[i] = unCompValues[i].getNew();
  // values[i].setValueInBytes(fileHolder.readByteArray(fileName,
  // measuresOffsetsArray[i],
  // measuresLengthArray[i]));
  // }
  // }
  // }

  /**
   * DoubleArrayDataInMemoryStore.
   *
   * @param size
   * @param elementSize
   * @param compressionModel
   * @param measuresOffsetsArray
   * @param measuresLengthArray
   * @param fileName
   * @param fileHolder
   */
  public DoubleArrayDataInMemoryStore(ValueCompressionModel compressionModel,
      long[] measuresOffsetsArray, int[] measuresLengthArray, String fileName,
      FileHolder fileHolder) {
    super(compressionModel);
    if (null != compressionModel) {
      UnCompressValue[] unCompValues = compressionModel.getUnCompressValues();
      if (null != unCompValues) {
        for (int i = 0; i < measuresLengthArray.length; i++) {

          values[i] = unCompValues[i].getNew();
          values[i].setValueInBytes(
              fileHolder.readByteArray(fileName, measuresOffsetsArray[i], measuresLengthArray[i]));
        }
      }
    }
  }

  /**
   * DoubleArrayDataInMemoryStore.
   *
   * @param size
   * @param elementSize
   * @param compressionModel
   * @param measuresOffsetsArray
   * @param measuresLengthArray
   * @param fileName
   * @param fileHolder
   */
  public DoubleArrayDataInMemoryStore(ValueCompressionModel compressionModel) {
    super(compressionModel);
  }

  @Override public MeasureDataWrapper getBackData(int[] cols, FileHolder fileHolder) {
    if (null == compressionModel) {
      return null;
    }
    CarbonReadDataHolder[] vals = new CarbonReadDataHolder[values.length];
    if (null == cols) {
      for (int i = 0; i < vals.length; i++) {
        vals[i] = values[i]
            .getValues(compressionModel.getDecimal()[i], compressionModel.getMaxValue()[i]);
      }
    } else {
      for (int i = 0; i < cols.length; i++) {
        vals[cols[i]] = values[cols[i]].getValues(compressionModel.getDecimal()[cols[i]],
            compressionModel.getMaxValue()[cols[i]]);
      }
    }
    // return new CompressedDataMeasureDataWrapper(values,
    // compressionModel.getDecimal(), compressionModel.getMaxValue());
    return new CompressedDataMeasureDataWrapper(vals);
  }

  @Override public MeasureDataWrapper getBackData(int cols, FileHolder fileHolder) {
    if (null == compressionModel) {
      return null;
    }
    CarbonReadDataHolder[] vals = new CarbonReadDataHolder[values.length];

    vals[cols] = values[cols]
        .getValues(compressionModel.getDecimal()[cols], compressionModel.getMaxValue()[cols]);
    return new CompressedDataMeasureDataWrapper(vals);
  }

}
