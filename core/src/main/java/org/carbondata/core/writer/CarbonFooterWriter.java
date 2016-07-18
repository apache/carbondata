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
package org.carbondata.core.writer;

import java.io.IOException;

import org.carbondata.format.FileFooter;

/**
 * Writes metadata block to the fact table file in thrift format org.carbondata.format.FileFooter
 */
public class CarbonFooterWriter {

  // It is version number of this format class.
  private static int VERSION_NUMBER = 1;

  // Fact file path
  private String filePath;

  public CarbonFooterWriter(String filePath) {
    this.filePath = filePath;
  }

  /**
   * It writes FileFooter thrift format object to file.
   *
   * @param footer
   * @param currentPosition At where this metadata is going to be written.
   * @throws IOException
   */
  public void writeFooter(FileFooter footer, long currentPosition) throws IOException {

    ThriftWriter thriftWriter = openThriftWriter(filePath);
    footer.setVersion(VERSION_NUMBER);
    try {
      thriftWriter.write(footer);
      thriftWriter.writeOffset(currentPosition);
    } catch (Exception e) {
      throw e;
    } finally {
      thriftWriter.close();
    }
  }

  /**
   * open thrift writer for writing dictionary chunk/meta object
   */
  private ThriftWriter openThriftWriter(String filePath) throws IOException {
    // create thrift writer instance
    ThriftWriter thriftWriter = new ThriftWriter(filePath, true);
    // open the file stream
    thriftWriter.open();
    return thriftWriter;
  }
}
