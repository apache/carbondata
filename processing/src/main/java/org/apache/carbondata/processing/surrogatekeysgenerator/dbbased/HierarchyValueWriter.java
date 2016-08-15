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

package org.apache.carbondata.processing.surrogatekeysgenerator.dbbased;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

import org.pentaho.di.core.exception.KettleException;

public class HierarchyValueWriter {

  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(HierarchyValueWriter.class.getName());
  /**
   * BUFFFER_SIZE
   */
  private static final int BUFFFER_SIZE = 32768;
  /**
   * hierarchyName
   */
  private String hierarchyName;
  /**
   * bufferedOutStream
   */
  private BufferedOutputStream bufferedOutStream;
  /**
   * storeFolderLocation
   */
  private String storeFolderLocation;

  /**
   * byteArrayList
   */
  private List<byte[]> byteArrayList = new ArrayList<byte[]>();

  public HierarchyValueWriter(String hierarchy, String storeFolderLocation) {
    this.hierarchyName = hierarchy;
    this.storeFolderLocation = storeFolderLocation;
  }

  /**
   * @return Returns the byteArrayList.
   */
  public List<byte[]> getByteArrayList() {
    return byteArrayList;
  }

  /**
   * @param byteArrayList The byteArrayList to set.
   */
  public void setByteArrayList(List<byte[]> byteArrayList) {
    this.byteArrayList = byteArrayList;
  }

  /**
   * @return Returns the bufferedOutStream.
   */
  public BufferedOutputStream getBufferedOutStream() {
    return bufferedOutStream;
  }

  public void writeIntoHierarchyFile(byte[] bytes) throws KettleException {
    File f = new File(storeFolderLocation + File.separator + hierarchyName);

    FileOutputStream fos = null;

    boolean isFileCreated = false;
    if (!f.exists()) {
      try {
        isFileCreated = f.createNewFile();

      } catch (IOException e) {
        throw new KettleException("unable to create file", e);
      }
      if (!isFileCreated) {
        throw new KettleException("unable to create file" + f.getAbsolutePath());
      }
    }
    try {
      if (null == bufferedOutStream) {
        fos = new FileOutputStream(f);
        bufferedOutStream = new BufferedOutputStream(fos, BUFFFER_SIZE);

      }
      bufferedOutStream.write(bytes);

    } catch (FileNotFoundException e) {
      closeStreamAndDeleteFile(f, bufferedOutStream, fos);
      throw new KettleException("hierarchy mapping file not found", e);
    } catch (IOException e) {
      closeStreamAndDeleteFile(f, bufferedOutStream, fos);
      throw new KettleException("Error while writting in the hierarchy mapping file", e);
    }
  }

  private void closeStreamAndDeleteFile(File f, Closeable... streams) throws KettleException {
    boolean isDeleted = false;
    for (Closeable stream : streams) {
      if (null != stream) {
        try {
          stream.close();
        } catch (IOException e) {
          LOGGER.error(e,
              "unable to close the stream ");
        }

      }
    }

    // delete the file
    isDeleted = f.delete();
    if (!isDeleted) {
      LOGGER.error("Unable to delete the file " + f.getAbsolutePath());
    }

  }

  /**
   * @return Returns the hierarchyName.
   */
  public String getHierarchyName() {
    return hierarchyName;
  }

}

