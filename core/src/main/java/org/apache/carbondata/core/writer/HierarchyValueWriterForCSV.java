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

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

import org.pentaho.di.core.exception.KettleException;

public class HierarchyValueWriterForCSV {

  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(HierarchyValueWriterForCSV.class.getName());
  /**
   * hierarchyName
   */
  private String hierarchyName;

  /**
   * bufferedOutStream
   */
  private FileChannel outPutFileChannel;

  /**
   * storeFolderLocation
   */
  private String storeFolderLocation;

  /**
   * intialized
   */
  private boolean intialized;

  /**
   * counter the number of files.
   */
  private int counter;

  /**
   * byteArrayList
   */
  private List<ByteArrayHolder> byteArrayholder =
      new ArrayList<ByteArrayHolder>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

  /**
   * toflush
   */
  private int toflush;

  public HierarchyValueWriterForCSV(String hierarchy, String storeFolderLocation) {
    this.hierarchyName = hierarchy;
    this.storeFolderLocation = storeFolderLocation;

    CarbonProperties instance = CarbonProperties.getInstance();

    this.toflush = Integer.parseInt(instance
        .getProperty(CarbonCommonConstants.SORT_SIZE, CarbonCommonConstants.SORT_SIZE_DEFAULT_VAL));

    int rowSetSize = Integer.parseInt(instance.getProperty(CarbonCommonConstants.GRAPH_ROWSET_SIZE,
        CarbonCommonConstants.GRAPH_ROWSET_SIZE_DEFAULT));

    if (this.toflush > rowSetSize) {
      this.toflush = rowSetSize;
    }

    updateCounter(hierarchy, storeFolderLocation);
  }

  /**
   * @return Returns the byteArrayList.
   */
  public List<ByteArrayHolder> getByteArrayList() {
    return byteArrayholder;
  }

  public FileChannel getBufferedOutStream() {
    return outPutFileChannel;
  }

  private void updateCounter(final String meString, String storeFolderLocation) {
    File storeFolder = new File(storeFolderLocation);

    File[] listFiles = storeFolder.listFiles(new FileFilter() {

      @Override public boolean accept(File file) {
        if (file.getName().indexOf(meString) > -1)

        {
          return true;
        }
        return false;
      }
    });

    if (null == listFiles || listFiles.length == 0) {
      counter = 0;
      return;
    }

    for (File hierFile : listFiles) {
      String hierFileName = hierFile.getName();

      if (hierFileName.endsWith(CarbonCommonConstants.FILE_INPROGRESS_STATUS)) {
        hierFileName = hierFileName.substring(0, hierFileName.lastIndexOf('.'));
        try {
          counter = Integer.parseInt(hierFileName.substring(hierFileName.length() - 1));
        } catch (NumberFormatException nfe) {

          if (new File(hierFileName + '0' + CarbonCommonConstants.LEVEL_FILE_EXTENSION).exists()) {
            // Need to skip because the case can come in which server went down while files were
            // merging and the other hierarchy files were not deleted, and the current file
            // status is inrogress. so again we will merge the files and rename to normal file
            LOGGER.info("Need to skip as this can be case in which hierarchy file already renamed");
            if (hierFile.delete()) {
              LOGGER.info("Deleted the Inprogress hierarchy Files.");
            }
          } else {
            // levelfileName0.level file not exist that means files is merged and other
            // files got deleted. while renaming this file from inprogress to normal file,
            // server got restarted/killed. so we need to rename the file to normal.

            File inprogressFile = new File(storeFolder + File.separator + hierFile.getName());
            File changetoName = new File(storeFolder + File.separator + hierFileName);

            if (inprogressFile.renameTo(changetoName)) {
              LOGGER.info(
                  "Renaming the level Files while creating the new instance on server startup.");
            }

          }

        }
      }

      String val = hierFileName.substring(hierFileName.length() - 1);

      int parsedVal = getIntValue(val);

      if (counter < parsedVal) {
        counter = parsedVal;
      }
    }
    counter++;
  }

  private int getIntValue(String val) {
    int parsedVal = 0;
    try {
      parsedVal = Integer.parseInt(val);
    } catch (NumberFormatException nfe) {
      LOGGER.info("Hierarchy File is already renamed so there will not be"
              + "any need to keep the counter");
    }
    return parsedVal;
  }

  private void intialize() throws KettleException {
    intialized = true;

    File f = new File(storeFolderLocation + File.separator + hierarchyName + counter
        + CarbonCommonConstants.FILE_INPROGRESS_STATUS);

    counter++;

    FileOutputStream fos = null;

    boolean isFileCreated = false;
    if (!f.exists()) {
      try {
        isFileCreated = f.createNewFile();

      } catch (IOException e) {
        //not required: findbugs fix
        throw new KettleException("unable to create member mapping file", e);
      }
      if (!isFileCreated) {
        throw new KettleException("unable to create file" + f.getAbsolutePath());
      }
    }

    try {
      fos = new FileOutputStream(f);

      outPutFileChannel = fos.getChannel();
    } catch (FileNotFoundException e) {
      closeStreamAndDeleteFile(f, outPutFileChannel, fos);
      throw new KettleException("member Mapping File not found to write mapping info", e);
    }
  }

  public void writeIntoHierarchyFile(byte[] bytes, int primaryKey) throws KettleException {
    if (!intialized) {
      intialize();
    }

    ByteBuffer byteBuffer = storeValueInCache(bytes, primaryKey);

    try {
      byteBuffer.flip();
      outPutFileChannel.write(byteBuffer);
    } catch (IOException e) {
      throw new KettleException("Error while writting in the hierarchy mapping file", e);
    }
  }

  private ByteBuffer storeValueInCache(byte[] bytes, int primaryKey) {

    // adding 4 to store the total length of the row at the beginning
    ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 4);

    buffer.put(bytes);
    buffer.putInt(primaryKey);

    return buffer;
  }

  public void performRequiredOperation() throws KettleException {
    if (byteArrayholder.size() == 0) {
      return;
    }
    //write to the file and close the stream.
    Collections.sort(byteArrayholder);

    for (ByteArrayHolder byteArray : byteArrayholder) {
      writeIntoHierarchyFile(byteArray.getMdKey(), byteArray.getPrimaryKey());
    }

    CarbonUtil.closeStreams(outPutFileChannel);

    //rename the inprogress file to normal .level file
    String filePath = this.storeFolderLocation + File.separator + hierarchyName + (counter - 1)
        + CarbonCommonConstants.FILE_INPROGRESS_STATUS;
    File inProgressFile = new File(filePath);
    String inprogressFileName = inProgressFile.getName();

    String changedFileName = inprogressFileName.substring(0, inprogressFileName.lastIndexOf('.'));

    File orgFinalName = new File(this.storeFolderLocation + File.separator + changedFileName);

    if (!inProgressFile.renameTo(orgFinalName)) {
      LOGGER.error("Not able to rename file : " + inprogressFileName);
    }

    //create the new outputStream
    try {
      intialize();
    } catch (KettleException e) {
      LOGGER.error("Not able to create output stream for file:" + hierarchyName + (counter - 1));
    }

    //clear the byte array holder also.
    byteArrayholder.clear();
  }

  private void closeStreamAndDeleteFile(File f, Closeable... streams) {
    boolean isDeleted = false;
    for (Closeable stream : streams) {
      if (null != stream) {
        try {
          stream.close();
        } catch (IOException e) {
          LOGGER.error(e, "unable to close the stream ");
        }

      }
    }

    // delete the file
    isDeleted = f.delete();
    if (!isDeleted) {
      LOGGER.error("Unable to delete the file " + f.getAbsolutePath());
    }

  }

  public String getHierarchyName() {
    return hierarchyName;
  }

  public int getCounter() {
    return counter;
  }

}

