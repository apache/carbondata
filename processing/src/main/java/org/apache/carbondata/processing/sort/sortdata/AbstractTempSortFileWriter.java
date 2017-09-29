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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.processing.sort.exception.CarbonSortKeyAndGroupByException;

public abstract class AbstractTempSortFileWriter implements TempSortFileWriter {

  /**
   * writeFileBufferSize
   */
  protected int writeBufferSize;

  /**
   * Measure count
   */
  protected int measureCount;

  /**
   * Measure count
   */
  protected int dimensionCount;

  /**
   * complexDimension count
   */
  protected int complexDimensionCount;

  /**
   * stream
   */
  protected DataOutputStream stream;

  /**
   * noDictionaryCount
   */
  protected int noDictionaryCount;

  /**
   * AbstractTempSortFileWriter
   *
   * @param writeBufferSize
   * @param dimensionCount
   * @param measureCount
   */
  public AbstractTempSortFileWriter(int dimensionCount, int complexDimensionCount, int measureCount,
      int noDictionaryCount, int writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
    this.dimensionCount = dimensionCount;
    this.complexDimensionCount = complexDimensionCount;
    this.measureCount = measureCount;
    this.noDictionaryCount = noDictionaryCount;
  }

  /**
   * Below method will be used to initialize the stream and write the entry count
   */
  @Override public void initiaize(File file, int entryCount)
      throws CarbonSortKeyAndGroupByException {
    try {
      stream = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(file), writeBufferSize));
      stream.writeInt(entryCount);
    } catch (FileNotFoundException e1) {
      throw new CarbonSortKeyAndGroupByException(e1);
    } catch (IOException e) {
      throw new CarbonSortKeyAndGroupByException(e);
    }
  }

  /**
   * Below method will be used to close the stream
   */
  @Override public void finish() {
    CarbonUtil.closeStreams(stream);
  }
}
