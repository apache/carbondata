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

import java.io.File;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.impl.FileHolderImpl;

public abstract class AbstractSortTempFileReader {
  /**
   * measure count
   */
  protected int measureCount;

  /**
   * mdKeyLenght
   */
  protected int mdKeyLenght;

  /**
   * isFactMdkeyInSort
   */
  protected boolean isFactMdkeyInSort;

  /**
   * factMdkeyLength
   */
  protected int factMdkeyLength;

  /**
   * entryCount
   */
  protected int entryCount;

  /**
   * fileHolder
   */
  protected FileHolder fileHolder;

  /**
   * filePath
   */
  protected String filePath;

  /**
   * eachRecordSize
   */
  protected int eachRecordSize;

  /**
   * type
   */
  protected char[] type;

  /**
   * CarbonCompressedSortTempFileReader
   *
   * @param measureCount
   * @param mdKeyLenght
   * @param isFactMdkeyInSort
   * @param factMdkeyLength
   * @param tempFile
   * @param type
   */
  public AbstractSortTempFileReader(int measureCount, int mdKeyLenght, boolean isFactMdkeyInSort,
      int factMdkeyLength, File tempFile, char[] type) {
    this.measureCount = measureCount;
    this.mdKeyLenght = mdKeyLenght;
    this.factMdkeyLength = factMdkeyLength;
    this.isFactMdkeyInSort = isFactMdkeyInSort;
    this.fileHolder = new FileHolderImpl(1);
    this.filePath = tempFile.getAbsolutePath();
    entryCount = fileHolder.readInt(filePath);
    eachRecordSize = measureCount + 1;
    this.type = type;
    if (isFactMdkeyInSort) {
      eachRecordSize += 1;
    }
  }

  /**
   * below method will be used to close the file holder
   */
  public void finish() {
    this.fileHolder.finish();
  }

  /**
   * Below method will be used to get the total row count in temp file
   *
   * @return
   */
  public int getEntryCount() {
    return entryCount;
  }

  /**
   * Below method will be used to get the row
   */
  public abstract Object[][] getRow();
}
