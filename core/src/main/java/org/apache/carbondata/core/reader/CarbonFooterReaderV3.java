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

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.format.FileFooter3;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;

/**
 * Below class to read file footer of version3
 * carbon data file
 */
public class CarbonFooterReaderV3 {

  //Fact file path
  private String filePath;

  // size of the file
  private long fileSize;

  //start offset of the file footer
  private long footerOffset;

  public CarbonFooterReaderV3(String filePath, long fileSize, long offset) {
    this.filePath = filePath;
    this.fileSize = fileSize;
    this.footerOffset = offset;
  }

  /**
   * It reads the metadata in FileFooter thrift object format.
   *
   * @return
   * @throws IOException
   */
  public FileFooter3 readFooterVersion3() throws IOException {
    ThriftReader thriftReader = openThriftReader(filePath);
    thriftReader.open();

    // If footer offset is 0, means caller does not set it,
    // so we read it from the end of the file
    if (footerOffset == 0) {
      Configuration conf = FileFactory.getConfiguration();
      FileFactory.FileType fileType = FileFactory.getFileType(filePath);
      DataInputStream reader = FileFactory.getDataInputStream(filePath, fileType, conf);
      long skipBytes = fileSize - CarbonCommonConstants.LONG_SIZE_IN_BYTE;
      long skipped = reader.skip(skipBytes);
      if (skipped != skipBytes) {
        throw new IOException(String.format(
            "expect skip %d bytes, but actually skipped %d bytes", skipBytes, skipped));
      }
      footerOffset = reader.readLong();
      reader.close();
    }

    // Set the offset from where it should read
    thriftReader.setReadOffset(footerOffset);
    FileFooter3 footer = (FileFooter3) thriftReader.read();
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

    return new ThriftReader(filePath, new ThriftReader.TBaseCreator() {
      @Override
      public TBase create() {
        return new FileFooter3();
      }
    });
  }

}
