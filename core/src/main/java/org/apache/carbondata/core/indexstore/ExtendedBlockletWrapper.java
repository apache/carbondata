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

package org.apache.carbondata.core.indexstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.compression.SnappyCompressor;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.stream.ExtendedByteArrayInputStream;
import org.apache.carbondata.core.stream.ExtendedByteArrayOutputStream;
import org.apache.carbondata.core.stream.ExtendedDataInputStream;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.log4j.Logger;

public class ExtendedBlockletWrapper implements Writable, Serializable {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ExtendedBlockletWrapper.class.getName());

  private boolean isWrittenToFile;

  private int dataSize;

  private byte[] bytes;

  private static final int BUFFER_SIZE = 8 * 1024 * 1024;

  private static final int BLOCK_SIZE = 256 * 1024 * 1024;

  public ExtendedBlockletWrapper() {

  }

  public ExtendedBlockletWrapper(List<ExtendedBlocklet> extendedBlockletList, String tablePath,
      String queryId) {
    Map<String, Byte> uniqueLocations = new HashMap<>();
    byte[] bytes = convertToBytes(tablePath, uniqueLocations, extendedBlockletList);
    int serializeAllowedSize = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_INDEX_SERVER_SERIALIZATION_THRESHOLD)) * 1024;
    DataOutputStream stream = null;
    if (bytes.length > serializeAllowedSize) {
      final String fileName = UUID.randomUUID().toString();
      try {
        String folderPath = tablePath + "/" + queryId;
        final CarbonFile carbonFile = FileFactory.getCarbonFile(folderPath);
        boolean writeToFile = false;
        if (carbonFile.isFileExist(folderPath)) {
          writeToFile = true;
        }
        if (writeToFile) {
          stream = FileFactory
              .getDataOutputStream(folderPath + "/" + fileName, FileFactory.getFileType(folderPath),
                  BUFFER_SIZE, BLOCK_SIZE, (short) 1);
          writeBlockletToStream(stream, bytes, uniqueLocations, extendedBlockletList);
          this.dataSize = stream.size();
          this.bytes = fileName.getBytes(CarbonCommonConstants.DEFAULT_CHARSET);
          isWrittenToFile = true;
        }
      } catch (IOException e) {
        LOGGER.error("Problem while writing to file, data will be sent through network", e);
      } finally {
        CarbonUtil.closeStreams(stream);
      }
    }
    if (!isWrittenToFile) {
      try {
        ExtendedByteArrayOutputStream bos = new ExtendedByteArrayOutputStream();
        stream = new DataOutputStream(bos);
        writeBlockletToStream(stream, bytes, uniqueLocations, extendedBlockletList);
        this.dataSize = bos.size();
        this.bytes = bos.getBuffer();
      } catch (IOException e) {
        LOGGER.error("Problem while writing data to memory stream", e);
      } finally {
        CarbonUtil.closeStreams(stream);
      }
    }
  }

  private byte[] convertToBytes(String tablePath, Map<String, Byte> uniqueLocations,
      List<ExtendedBlocklet> extendedBlockletList) {
    ByteArrayOutputStream bos = new ExtendedByteArrayOutputStream();
    DataOutputStream stream = new DataOutputStream(bos);
    try {
      for (ExtendedBlocklet extendedBlocklet : extendedBlockletList) {
        extendedBlocklet.setFilePath(extendedBlocklet.getFilePath().replace(tablePath, ""));
        extendedBlocklet.serializeData(stream, uniqueLocations);
      }
      return new SnappyCompressor().compressByte(bos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      CarbonUtil.closeStreams(stream);
    }
  }

  private void writeBlockletToStream(DataOutputStream stream, byte[] data,
      Map<String, Byte> uniqueLocation, List<ExtendedBlocklet> extendedBlockletList)
      throws IOException {
    stream.writeInt(extendedBlockletList.size());
    String[] uniqueLoc = new String[uniqueLocation.size()];
    Iterator<Map.Entry<String, Byte>> iterator = uniqueLocation.entrySet().iterator();
    while (iterator.hasNext()) {
      final Map.Entry<String, Byte> next = iterator.next();
      uniqueLoc[next.getValue()] = next.getKey();
    }
    stream.writeByte((byte) uniqueLoc.length);
    for (String loc : uniqueLoc) {
      stream.writeUTF(loc);
    }
    stream.writeInt(data.length);
    stream.write(data);
  }

  public List<ExtendedBlocklet> readBlocklet(String tablePath, String queryId) throws IOException {
    byte[] data;
    if (isWrittenToFile) {
      DataInputStream stream = null;
      try {
        String fileName = new String(bytes, CarbonCommonConstants.DEFAULT_CHARSET);
        stream = FileFactory.getDataInputStream(tablePath + "/" + queryId + "/" + fileName,
            FileFactory.getFileType(tablePath));
        data = new byte[dataSize];
        stream.readFully(data);
      } finally {
        CarbonUtil.closeStreams(stream);
      }
    } else {
      data = bytes;
    }
    DataInputStream stream = null;
    int numberOfBlocklet;
    String[] locations;
    int actualDataLen;
    try {
      stream = new DataInputStream(new ByteArrayInputStream(data));
      numberOfBlocklet = stream.readInt();
      final byte numberOfLocations = stream.readByte();
      locations = new String[numberOfLocations];
      for (int i = 0; i < numberOfLocations; i++) {
        locations[i] = stream.readUTF();
      }
      actualDataLen = stream.readInt();
    } finally {
      CarbonUtil.closeStreams(stream);
    }

    final byte[] unCompressByte =
        new SnappyCompressor().unCompressByte(data, data.length - actualDataLen, actualDataLen);
    ExtendedByteArrayInputStream ebis = new ExtendedByteArrayInputStream(unCompressByte);
    ExtendedDataInputStream eDIS = new ExtendedDataInputStream(ebis);
    List<ExtendedBlocklet> extendedBlockletList = new ArrayList<>();
    try {
      for (int i = 0; i < numberOfBlocklet; i++) {
        ExtendedBlocklet extendedBlocklet = new ExtendedBlocklet();
        extendedBlocklet.deserializeFields(eDIS, locations, tablePath);
        extendedBlockletList.add(extendedBlocklet);
      }
    } finally {
      CarbonUtil.closeStreams(eDIS);
    }
    return extendedBlockletList;
  }

  @Override public void write(DataOutput out) throws IOException {
    out.writeBoolean(isWrittenToFile);
    out.writeInt(bytes.length);
    out.write(bytes);
    out.writeInt(dataSize);
  }

  @Override public void readFields(DataInput in) throws IOException {
    this.isWrittenToFile = in.readBoolean();
    this.bytes = new byte[in.readInt()];
    in.readFully(bytes);
    this.dataSize = in.readInt();
  }
}
