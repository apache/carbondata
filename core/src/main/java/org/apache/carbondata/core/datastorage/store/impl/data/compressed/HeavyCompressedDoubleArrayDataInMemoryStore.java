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

package org.apache.carbondata.core.datastorage.store.impl.data.compressed;

import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.MeasureDataWrapper;
import org.apache.carbondata.core.datastorage.store.compression.WriterCompressModel;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.datastorage.store.impl.CompressedDataMeasureDataWrapper;

public class HeavyCompressedDoubleArrayDataInMemoryStore
    extends AbstractHeavyCompressedDoubleArrayDataStore {

  public HeavyCompressedDoubleArrayDataInMemoryStore(WriterCompressModel compressionModel) {
    super(compressionModel);
  }

  @Override public MeasureDataWrapper getBackData(int[] cols, FileHolder fileHolder) {
    if (null == compressionModel) {
      return null;
    }
    CarbonReadDataHolder[] vals = new CarbonReadDataHolder[values.length];
    if (cols != null) {
      for (int i = 0; i < cols.length; i++) {
        vals[cols[i]] = values[cols[i]].uncompress(compressionModel.getConvertedDataType()[cols[i]])
            .getValues(compressionModel.getMantissa()[cols[i]],
                compressionModel.getMaxValue()[cols[i]]);
      }
    } else {
      for (int i = 0; i < vals.length; i++) {

        vals[i] = values[i].uncompress(compressionModel.getConvertedDataType()[i])
            .getValues(compressionModel.getMantissa()[i], compressionModel.getMaxValue()[i]);
      }
    }
    return new CompressedDataMeasureDataWrapper(vals);
  }

  @Override public MeasureDataWrapper getBackData(int cols, FileHolder fileHolder) {
    if (null == compressionModel) {
      return null;
    }
    CarbonReadDataHolder[] vals = new CarbonReadDataHolder[values.length];
    vals[cols] = values[cols].uncompress(compressionModel.getConvertedDataType()[cols])
        .getValues(compressionModel.getMantissa()[cols], compressionModel.getMaxValue()[cols]);
    return new CompressedDataMeasureDataWrapper(vals);
  }
}
