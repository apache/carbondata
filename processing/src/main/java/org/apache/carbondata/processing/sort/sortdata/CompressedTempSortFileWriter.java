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

package org.apache.carbondata.processing.sort.sortdata;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;

public class CompressedTempSortFileWriter extends AbstractTempSortFileWriter {

  /**
   * CompressedTempSortFileWriter
   *
   * @param writeBufferSize
   * @param dimensionCount
   * @param measureCount
   */
  public CompressedTempSortFileWriter(int dimensionCount, int complexDimensionCount,
      int measureCount, int noDictionaryCount, int writeBufferSize) {
    super(dimensionCount, complexDimensionCount, measureCount, noDictionaryCount, writeBufferSize);
  }

  /**
   * Below method will be used to write the sort temp file
   *
   * @param records
   */
  public void writeSortTempFile(Object[][] records) throws CarbonSortKeyAndGroupByException {
    DataOutputStream dataOutputStream = null;
    ByteArrayOutputStream blockDataArray = null;
    int totalSize = 0;
    int recordSize = 0;
    try {
      recordSize = (measureCount * CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE) + (dimensionCount
          * CarbonCommonConstants.INT_SIZE_IN_BYTE);
      totalSize = records.length * recordSize;

      blockDataArray = new ByteArrayOutputStream(totalSize);
      dataOutputStream = new DataOutputStream(blockDataArray);

      UnCompressedTempSortFileWriter
          .writeDataOutputStream(records, dataOutputStream, measureCount, dimensionCount,
              noDictionaryCount, complexDimensionCount);

      stream.writeInt(records.length);
      byte[] byteArray = CompressorFactory.getInstance().getCompressor()
          .compressByte(blockDataArray.toByteArray());
      stream.writeInt(byteArray.length);
      stream.write(byteArray);

    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException(e);
    } finally {
      CarbonUtil.closeStreams(blockDataArray);
      CarbonUtil.closeStreams(dataOutputStream);
    }
  }
}
