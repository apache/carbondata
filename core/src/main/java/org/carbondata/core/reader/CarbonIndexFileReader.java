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
package org.carbondata.core.reader;

import java.io.IOException;

import org.carbondata.format.BlockIndex;
import org.carbondata.format.IndexHeader;

import org.apache.thrift.TBase;

/**
 * Reader class which will be used to read the index file
 */
public class CarbonIndexFileReader {

  /**
   * reader
   */
  private ThriftReader thriftReader;

  /**
   * Below method will be used to read the index header
   *
   * @return index header
   * @throws IOException if any problem  while reader the header
   */
  public IndexHeader readIndexHeader() throws IOException {
    IndexHeader indexHeader = (IndexHeader) thriftReader.read(new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new IndexHeader();
      }
    });
    return indexHeader;
  }

  /**
   * Below method will be used to close the reader
   */
  public void closeThriftReader() {
    thriftReader.close();
  }

  /**
   * Below method will be used to read the block index from fie
   *
   * @return block index info
   * @throws IOException if problem while reading the block index
   */
  public BlockIndex readBlockIndexInfo() throws IOException {
    BlockIndex blockInfo = (BlockIndex) thriftReader.read(new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new BlockIndex();
      }
    });
    return blockInfo;
  }

  /**
   * Open the thrift reader
   *
   * @param filePath
   * @throws IOException
   */
  public void openThriftReader(String filePath) throws IOException {
    thriftReader = new ThriftReader(filePath);
    thriftReader.open();
  }

  /**
   * check if any more object is present
   *
   * @return true if any more object can be read
   * @throws IOException
   */
  public boolean hasNext() throws IOException {
    return thriftReader.hasNext();
  }
}
