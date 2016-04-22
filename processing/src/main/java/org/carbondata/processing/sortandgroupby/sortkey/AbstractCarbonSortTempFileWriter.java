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

package org.carbondata.processing.sortandgroupby.sortkey;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.sortandgroupby.exception.CarbonSortKeyAndGroupByException;

public abstract class AbstractCarbonSortTempFileWriter implements CarbonSortTempFileWriter {
  /**
   * Measure count
   */
  protected int measureCount;

  /**
   * mdkeyIndex
   */
  protected int mdkeyIndex;

  /**
   * isFactMdkeyInSort
   */
  protected boolean isFactMdkeyInSort;

  /**
   * factMdkeyLength
   */
  protected int factMdkeyLength;

  /**
   * writeFileBufferSize
   */
  protected int writeFileBufferSize;

  /**
   * stream
   */
  protected DataOutputStream stream;

  /**
   * mdKeyLength
   */
  protected int mdKeyLength;

  /**
   * type
   */
  protected char[] type;

  /**
   * AbstractCarbonSortTempFileWriter
   *
   * @param measureCount
   * @param mdkeyIndex
   * @param mdKeyLength
   * @param isFactMdkeyInSort
   * @param factMdkeyLength
   * @param writeFileBufferSize
   */
  public AbstractCarbonSortTempFileWriter(int measureCount, int mdkeyIndex, int mdKeyLength,
      boolean isFactMdkeyInSort, int factMdkeyLength, int writeFileBufferSize, char[] type) {
    this.measureCount = measureCount;
    this.mdkeyIndex = mdkeyIndex;
    this.isFactMdkeyInSort = isFactMdkeyInSort;
    this.factMdkeyLength = factMdkeyLength;
    this.writeFileBufferSize = writeFileBufferSize;
    this.mdKeyLength = mdKeyLength;
    this.type = type;
  }

  /**
   * Below method will be used to initialise the stream and write the entry count
   */
  @Override public void initiaize(File file, int entryCount)
      throws CarbonSortKeyAndGroupByException {
    try {
      stream = new DataOutputStream(
          new BufferedOutputStream(new FileOutputStream(file), writeFileBufferSize));
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
