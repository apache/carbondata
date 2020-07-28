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

package org.apache.carbondata.core.reader;

import java.io.IOException;

import org.apache.carbondata.format.FileFooter;

/**
 * Reads the metadata from fact file in org.apache.carbondata.format.FileFooter thrift object
 */
public class CarbonFooterReader {

  //Fact file path
  private String filePath;

  //From which offset of file this metadata should be read
  private long offset;

  public CarbonFooterReader(String filePath, long offset) {

    this.filePath = filePath;
    this.offset = offset;
  }

  /**
   * It reads the metadata in FileFooter thrift object format.
   *
   * @return
   * @throws IOException
   */
  public FileFooter readFooter() throws IOException {
    ThriftReader thriftReader = openThriftReader(filePath);
    thriftReader.open();
    //Set the offset from where it should read
    thriftReader.setReadOffset(offset);
    FileFooter footer = (FileFooter) thriftReader.read();
    thriftReader.close();
    return footer;
  }

  /**
   * Open the thrift reader
   *
   * @param filePath
   * @return
   * @throws IOException
   */
  private ThriftReader openThriftReader(String filePath) {
    return new ThriftReader(filePath, FileFooter::new);
  }

}
