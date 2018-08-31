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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

/**
 * A simple class for reading Thrift objects (of a single type) from a fileName.
 */
public class ThriftReader {
  /**
   * buffer size
   */
  private static final int bufferSize = 2048;
  /**
   * File containing the objects.
   */
  private String fileName;
  /**
   * Used to create empty objects that will be initialized with values from the fileName.
   */
  private TBaseCreator creator;
  /**
   * For reading the fileName.
   */
  private DataInputStream dataInputStream;
  /**
   * For reading the binary thrift objects.
   */
  private TProtocol binaryIn;

  private Configuration configuration;

  /**
   * Constructor.
   */
  public ThriftReader(String fileName, TBaseCreator creator) {
    this.fileName = fileName;
    this.creator = creator;
  }

  public ThriftReader(String fileName, TBaseCreator creator, Configuration configuration) {
    this.fileName = fileName;
    this.configuration = configuration;
    this.creator = creator;
  }

  /**
   * Constructor.
   */
  public ThriftReader(String fileName) {
    this.fileName = fileName;
  }

  /**
   * Constructor.
   */
  public ThriftReader(String fileName, Configuration configuration) {
    this.fileName = fileName;
    this.configuration = configuration;
  }

  /**
   * Constructor.
   */
  public ThriftReader(byte[] fileData) {
    dataInputStream = new DataInputStream(new ByteArrayInputStream(fileData));
    binaryIn = new TCompactProtocol(new TIOStreamTransport(dataInputStream));
  }

  /**
   * Opens the fileName for reading.
   */
  public void open() throws IOException {
    Configuration conf = configuration != null ? configuration : FileFactory.getConfiguration();
    FileFactory.FileType fileType = FileFactory.getFileType(fileName);
    dataInputStream = FileFactory.getDataInputStream(fileName, fileType, bufferSize, conf);
    binaryIn = new TCompactProtocol(new TIOStreamTransport(dataInputStream));
  }

  /**
   * This method will set the position of stream from where data has to be read
   */
  public void setReadOffset(long bytesToSkip) throws IOException {
    if (dataInputStream.skip(bytesToSkip) != bytesToSkip) {
      throw new IOException("It doesn't set the offset properly");
    }
  }

  /**
   * Checks if another objects is available by attempting to read another byte from the stream.
   */
  public boolean hasNext() throws IOException {
    dataInputStream.mark(1);
    int val = dataInputStream.read();
    dataInputStream.reset();
    return val != -1;
  }

  /**
   * Reads the next object from the fileName.
   */
  public TBase read() throws IOException {
    TBase t = creator.create();
    try {
      t.read(binaryIn);
    } catch (TException e) {
      throw new IOException(e);
    }
    return t;
  }

  /**
   * Reads the next object from the fileName.
   *
   * @param creator type of object which will be returned
   * @throws IOException any problem while reading
   */
  public TBase read(TBaseCreator creator) throws IOException {
    TBase t = creator.create();
    try {
      t.read(binaryIn);
    } catch (TException e) {
      throw new IOException(e);
    }
    return t;
  }

  /**
   * Close the fileName.
   */
  public void close() {
    CarbonUtil.closeStreams(dataInputStream);
  }

  /**
   * Thrift deserializes by taking an existing object and populating it. ThriftReader
   * needs a way of obtaining instances of the class to be populated and this interface
   * defines the mechanism by which a client provides these instances.
   */
  public interface TBaseCreator {
    TBase create();
  }
}
