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
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;

public class UnCompressedTempSortFileWriter extends AbstractTempSortFileWriter {

  /**
   * UnCompressedTempSortFileWriter
   *
   * @param tableFieldStat
   * @param writeBufferSize
   */
  public UnCompressedTempSortFileWriter(TableFieldStat tableFieldStat, int writeBufferSize) {
    super(tableFieldStat, writeBufferSize);
  }

  /**
   * write records to stream
   * @param records records
   * @param dataOutputStream out stream
   * @throws IOException
   */
  private void writeDataOutputStream(Object[][] records, DataOutputStream dataOutputStream)
      throws IOException {
    for (int rowIdx = 0; rowIdx < records.length; rowIdx++) {
      this.sortStepRowHandler.writePartedRowToOutputStream(records[rowIdx], dataOutputStream);
    }
  }

  /**
   * Below method will be used to write the sort temp file
   *
   * @param records
   */
  public void writeSortTempFile(Object[][] records) throws CarbonSortKeyAndGroupByException {
    ByteArrayOutputStream blockDataArray = null;
    DataOutputStream dataOutputStream = null;
    int totalSize = 0;
    int recordSize = 0;
    try {
      recordSize =
          (tableFieldStat.getMeasureCnt() * CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE) + (
              tableFieldStat.getDimCnt() * CarbonCommonConstants.INT_SIZE_IN_BYTE);
      totalSize = records.length * recordSize;

      blockDataArray = new ByteArrayOutputStream(totalSize);
      dataOutputStream = new DataOutputStream(blockDataArray);

      writeDataOutputStream(records, dataOutputStream);
      stream.writeInt(records.length);
      byte[] byteArray = blockDataArray.toByteArray();
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
