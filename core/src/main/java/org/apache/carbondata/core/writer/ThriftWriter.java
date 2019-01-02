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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

/**
 * Simple class that makes it easy to write Thrift objects to disk.
 */
public class ThriftWriter {

  /**
   * buffer size
   */
  private static final int bufferSize = 2048;

  /**
   * File to write to.
   */
  private String fileName;

  /**
   * For writing to the file.
   */
  private DataOutputStream dataOutputStream;

  /**
   * For binary serialization of objects.
   */
  private TProtocol binaryOut;

  /**
   * Identifier for performing atomic file operations
   */
  private AtomicFileOperations atomicFileOperationsWriter;

  /**
   * flag to append to existing file
   */
  private boolean append;

  /**
   * Constructor.
   */
  public ThriftWriter(String fileName, boolean append) {
    this.fileName = fileName;
    this.append = append;
  }

  /**
   * Open the file for writing.
   */
  public void open() throws IOException {
    FileFactory.FileType fileType = FileFactory.getFileType(fileName);
    dataOutputStream = FileFactory.getDataOutputStream(fileName, fileType, bufferSize, append);
    binaryOut = new TCompactProtocol(new TIOStreamTransport(dataOutputStream));
  }

  /**
   * Method for opening file writing for atomic operations
   *
   * @param fileWriteOperation
   * @throws IOException
   */
  public void open(FileWriteOperation fileWriteOperation) throws IOException {
    atomicFileOperationsWriter = AtomicFileOperationFactory.getAtomicFileOperations(fileName);
    dataOutputStream = atomicFileOperationsWriter.openForWrite(fileWriteOperation);
    binaryOut = new TCompactProtocol(new TIOStreamTransport(dataOutputStream));
  }

  /**
   * This will check whether stream and binary out is open or not.
   * @return
   */
  public boolean isOpen() {
    if (null != binaryOut && null != dataOutputStream) {
      return true;
    }
    return false;
  }

  /**
   * Write the object to disk.
   */
  public void write(TBase t) throws IOException {
    try {
      t.write(binaryOut);
      dataOutputStream.flush();
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  /**
   * Close the file stream.
   */
  public void close() throws IOException {
    closeAtomicFileWriter();
    CarbonUtil.closeStream(dataOutputStream);
  }

  /**
   * This method will close the atomic file operations writer
   *
   * @throws IOException
   */
  private void closeAtomicFileWriter() throws IOException {
    if (null != atomicFileOperationsWriter) {
      atomicFileOperationsWriter.close();
      // set output stream to null as atomic writer will close the data output stream internally
      dataOutputStream = null;
    }
  }
}
