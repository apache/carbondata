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

package org.apache.carbondata.core.writer;

import java.io.IOException;

import org.apache.thrift.TBase;

/**
 * Reader class which will be used to read the index file
 */
public class CarbonIndexFileWriter {

  /**
   * thrift writer object
   */
  private ThriftWriter thriftWriter;

  /**
   * It writes thrift object to file
   *
   * @throws IOException
   */
  public void writeThrift(TBase indexObject) throws IOException {
    thriftWriter.write(indexObject);
  }

  /**
   * Below method will be used to open the thrift writer
   *
   * @param filePath file path where data need to be written
   * @throws IOException throws io exception in case of any failure
   */
  public void openThriftWriter(String filePath) throws IOException {
    // create thrift writer instance
    thriftWriter = new ThriftWriter(filePath, true);
    // open the file stream
    thriftWriter.open();
  }

  /**
   * Below method will be used to close the thrift object
   */
  public void close() throws IOException {
    thriftWriter.close();
  }
}
