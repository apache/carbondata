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

package org.apache.carbondata.core.datastore.filesystem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

public class AlluxioCarbonFile extends HDFSCarbonFile {
  /**
   * LOGGER
   */
  private static final Logger LOGGER =
          LogServiceFactory.getLogService(AlluxioCarbonFile.class.getName());

  public AlluxioCarbonFile(String filePath) {
    super(filePath);
  }

  public AlluxioCarbonFile(Path path) {
    super(path);
  }

  public AlluxioCarbonFile(FileStatus fileStatus) {
    super(fileStatus);
  }

  /**
   * @param listStatus
   * @return
   */
  @Override
  protected CarbonFile[] getFiles(FileStatus[] listStatus) {
    if (listStatus == null) {
      return new CarbonFile[0];
    }
    CarbonFile[] files = new CarbonFile[listStatus.length];
    for (int i = 0; i < files.length; i++) {
      files[i] = new AlluxioCarbonFile(listStatus[i]);
    }
    return files;
  }

  @Override
  public CarbonFile[] listFiles(final CarbonFileFilter fileFilter) {
    CarbonFile[] files = listFiles();
    if (files != null && files.length >= 1) {
      List<CarbonFile> fileList = new ArrayList<CarbonFile>(files.length);
      for (int i = 0; i < files.length; i++) {
        if (fileFilter.accept(files[i])) {
          fileList.add(files[i]);
        }
      }
      if (fileList.size() >= 1) {
        return fileList.toArray(new CarbonFile[fileList.size()]);
      } else {
        return new CarbonFile[0];
      }
    }
    return files;
  }

  @Override
  public CarbonFile getParentFile() {
    Path parent = fileStatus.getPath().getParent();
    return null == parent ? null : new AlluxioCarbonFile(parent);
  }

  /**
   * <p>RenameForce of the fileName for the AlluxioFileSystem Implementation.
   * Involves by opening a {@link FSDataInputStream} from the existing path and copy
   * bytes to {@link FSDataOutputStream}.
   * </p>
   * <p>Close the output and input streams only after the files have been written
   * Also check for the existence of the changed path and then delete the previous Path.
   * The No of Bytes that can be read is controlled by {@literal io.file.buffer.size},
   * where the default value is 4096.</p>
   * @param changeToName
   * @return
   */
  @Override
  public boolean renameForce(String changeToName) {
    FileSystem fs = null;
    FSDataOutputStream fsdos = null;
    FSDataInputStream fsdis = null;
    try {
      Path actualPath = fileStatus.getPath();
      Path changedPath = new Path(changeToName);
      fs = actualPath.getFileSystem(hadoopConf);
      fsdos = fs.create(changedPath, true);
      fsdis = fs.open(actualPath);
      if (null != fsdis && null != fsdos) {
        try {
          IOUtils.copyBytes(fsdis, fsdos, hadoopConf, true);
          if (fs.exists(changedPath)) {
            fs.delete(actualPath, true);
          }
          // Reassigning fileStatus to the changedPath.
          fileStatus = fs.getFileStatus(changedPath);
        } catch (IOException e) {
          LOGGER.error("Exception occured: " + e.getMessage());
          return false;
        }
        return true;
      }
      return false;
    } catch (IOException e) {
      LOGGER.error("Exception occured: " + e.getMessage());
      return false;
    }
  }

  @Override
  public DataOutputStream getDataOutputStreamUsingAppend(String path, FileFactory.FileType fileType)
          throws IOException {
    return getDataOutputStream(path, fileType, CarbonCommonConstants.BYTEBUFFER_SIZE, true);
  }

  @Override
  public DataOutputStream getDataOutputStream(String path, FileFactory.FileType fileType,
                                              int bufferSize, boolean append) throws IOException {
    path = path.replace("\\", "/");
    Path pt = new Path(path);
    FileSystem fileSystem = pt.getFileSystem(FileFactory.getConfiguration());
    FSDataOutputStream stream;
    if (append) {
      if (CarbonUtil.isFileExists(path)) {
        DataInputStream dataInputStream = fileSystem.open(pt);
        int count = dataInputStream.available();
        // create buffer
        byte[] byteStreamBuffer = new byte[count];
        int bytesRead = dataInputStream.read(byteStreamBuffer);
        dataInputStream.close();
        stream = fileSystem.create(pt, true, bufferSize);
        if (bytesRead > 0) {
          stream.write(byteStreamBuffer, 0, bytesRead);
        }
      } else {
        stream = fileSystem.create(pt, true, bufferSize);
      }
    } else {
      stream = fileSystem.create(pt, true, bufferSize);
    }
    return stream;
  }
}
