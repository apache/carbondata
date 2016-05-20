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

package org.carbondata.core.util;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreInfo;
import org.carbondata.core.datastorage.store.columnar.UnBlockIndexer;
import org.carbondata.core.datastorage.store.compression.MeasureMetaDataModel;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperations;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperationsImpl;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.keygenerator.mdkey.NumberCompressor;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.metadata.BlockletInfo;
import org.carbondata.core.metadata.BlockletInfoColumnar;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.metadata.ValueEncoderMeta;
import org.carbondata.core.reader.CarbonFooterReader;
import org.carbondata.core.vo.ColumnGroupModel;
import org.carbondata.query.util.DataFileFooterConverter;

import com.google.gson.Gson;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import org.pentaho.di.core.exception.KettleException;


public final class CarbonUtil {

  private static final String HDFS_PREFIX = "hdfs://";

  private static final String FS_DEFAULT_FS = "fs.defaultFS";

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonUtil.class.getName());

  /**
   * EIGHT
   */
  private static final int CONST_EIGHT = 8;

  /**
   * SEVEN
   */
  private static final int CONST_SEVEN = 7;

  /**
   * HUNDRED
   */
  private static final int CONST_HUNDRED = 100;

  private static final Configuration conf = new Configuration(true);

  private CarbonUtil() {

  }

  /**
   * This method closes the streams
   *
   * @param streams - streams to close.
   */
  public static void closeStreams(Closeable... streams) {
    // Added if to avoid NullPointerException in case one stream is being passed as null
    if (null != streams) {
      for (Closeable stream : streams) {
        if (null != stream) {
          try {
            stream.close();
          } catch (IOException e) {
            LOGGER.error("Error while closing stream" + stream);
          }
        }
      }
    }
  }

  public static File[] getSortedFileList(File[] fileArray) {
    Arrays.sort(fileArray, new Comparator<File>() {
      public int compare(File o1, File o2) {
        try {
          return o1.getName().compareTo(o2.getName());
        } catch (Exception e) {

          LOGGER.error(e, "Error while getSortedFile");
          return 0;
        }
      }
    });
    return fileArray;
  }

  public static CarbonFile[] getSortedFileList(CarbonFile[] fileArray) {
    Arrays.sort(fileArray, new Comparator<CarbonFile>() {
      public int compare(CarbonFile o1, CarbonFile o2) {
        try {
          return o1.getName().compareTo(o2.getName());
        } catch (Exception e) {

          return o1.getName().compareTo(o2.getName());
        }
      }
    });
    return fileArray;
  }

  /**
   * @param baseStorePath
   * @return
   */
  private static int createBaseStoreFolders(String baseStorePath) {
    FileFactory.FileType fileType = FileFactory.getFileType(baseStorePath);
    try {
      if (!FileFactory.isFileExist(baseStorePath, fileType, false)) {
        if (!FileFactory.mkdirs(baseStorePath, fileType)) {
          return -1;
        }
      }
    } catch (Exception e) {
      return -1;
    }
    return 1;
  }

  /**
   * This method checks whether Restructure Folder exists or not
   * and if not exist then return the number with which folder need to created.
   *
   * @param baseStorePath -
   *                      baselocation where folder will be created.
   * @return counter
   * counter with which folder will be created.
   */
  public static int checkAndReturnCurrentRestructFolderNumber(String baseStorePath,
      final String filterType, final boolean isDirectory) {
    if (null == baseStorePath || 0 == baseStorePath.length()) {
      return -1;
    }
    // change the slashes to /
    baseStorePath = baseStorePath.replace("\\", "/");

    // check if string wnds with / then remove that.
    if (baseStorePath.charAt(baseStorePath.length() - 1) == '/') {
      baseStorePath = baseStorePath.substring(0, baseStorePath.lastIndexOf("/"));
    }
    int retValue = createBaseStoreFolders(baseStorePath);
    if (-1 == retValue) {
      return retValue;
    }

    CarbonFile carbonFile =
        FileFactory.getCarbonFile(baseStorePath, FileFactory.getFileType(baseStorePath));

    // List of directories
    CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile pathname) {
        if (isDirectory && pathname.isDirectory()) {
          if (pathname.getAbsolutePath().indexOf(filterType) > -1) {
            return true;
          }
        } else {
          if (pathname.getAbsolutePath().indexOf(filterType) > -1) {
            return true;
          }
        }

        return false;
      }
    });

    int counter = -1;

    // if no folder exists then return -1
    if (listFiles.length == 0) {
      return counter;
    }

    counter = findCounterValue(filterType, listFiles, counter);
    return counter;
  }

  public static int checkAndReturnCurrentLoadFolderNumber(String baseStorePath) {
    return checkAndReturnCurrentRestructFolderNumber(baseStorePath, "Load_", true);
  }

  /**
   * @param filterType
   * @param listFiles
   * @param counter
   * @return
   */
  private static int findCounterValue(final String filterType, CarbonFile[] listFiles,
      int counter) {
    if ("Load_".equals(filterType)) {
      for (CarbonFile files : listFiles) {
        String folderName = getFolderName(files);
        if (folderName.indexOf('.') > -1) {
          folderName = folderName.substring(0, folderName.indexOf('.'));
        }
        String[] split = folderName.split("_");

        if (split.length > 1 && counter < Integer.parseInt(split[1])) {
          counter = Integer.parseInt(split[1]);
        }
      }
    } else {
      // Iterate list of Directories and find the counter value
      for (CarbonFile eachFile : listFiles) {
        String folderName = getFolderName(eachFile);
        String[] split = folderName.split("_");
        if (counter < Integer.parseInt(split[1])) {
          counter = Integer.parseInt(split[1]);
        }
      }
    }
    return counter;
  }

  /**
   * @param eachFile
   * @return
   */
  private static String getFolderName(CarbonFile eachFile) {
    String str = eachFile.getAbsolutePath();
    str = str.replace("\\", "/");
    int firstFolderIndex = str.lastIndexOf("/");
    String folderName = str.substring(firstFolderIndex);
    return folderName;
  }

  /**
   * This method will be used to update the dimension cardinality
   *
   * @param dimCardinality
   * @return new increment cardinality
   */
  public static int[] getIncrementedCardinality(int[] dimCardinality) {
    // get the cardinality incr factor
    final int incrValue = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE,
            CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL));

    int perIncr = 0;
    int remainder = 0;
    int[] newDimsC = new int[dimCardinality.length];
    for (int i = 0; i < dimCardinality.length; i++) {
      // get the incr
      perIncr = (dimCardinality[i] * incrValue) / CONST_HUNDRED;

      // if per incr is more than one the add to cardinality
      if (perIncr > 0) {
        newDimsC[i] = dimCardinality[i] + perIncr;
      } else {
        // else add one
        newDimsC[i] = dimCardinality[i] + 1;
      }
      // check whether its in boundary condition
      remainder = newDimsC[i] % CONST_EIGHT;
      if (remainder == CONST_SEVEN) {
        // then incr cardinality by 1
        newDimsC[i] = dimCardinality[i] + 1;
      }
    }
    // get the log bits of cardinality
    for (int i = 0; i < newDimsC.length; i++) {
      newDimsC[i] = Long.toBinaryString(newDimsC[i]).length();
    }
    return newDimsC;
  }

  public static int getIncrementedCardinality(int dimCardinality) {
    // get the cardinality incr factor
    final int incrValue = Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE,
            CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL));

    int perIncr = 0;
    int remainder = 0;
    int newDimsC = 0;

    // get the incr
    perIncr = (dimCardinality * incrValue) / CONST_HUNDRED;

    // if per incr is more than one the add to cardinality
    if (perIncr > 0) {
      newDimsC = dimCardinality + perIncr;
    } else {
      // else add one
      newDimsC = dimCardinality + 1;
    }
    // check whether its in boundary condition
    remainder = newDimsC % CONST_EIGHT;
    if (remainder == CONST_SEVEN) {
      // then incr cardinality by 1
      newDimsC = dimCardinality + 1;
    }
    newDimsC = Long.toBinaryString(newDimsC).length();
    // get the log bits of cardinality

    return newDimsC;
  }

  /**
   * return ColumnGroupModel. check ColumnGroupModel for detail
   *
   * @param dimLens      : dimension cardinality
   * @param columnGroups : column groups
   * @return ColumnGroupModel  model
   */
  public static ColumnGroupModel getColGroupModel(int[] dimLens, int[][] columnGroups) {
    int[] columnSplit = new int[columnGroups.length];
    int noOfColumnStore = columnSplit.length;
    boolean[] columnarStore = new boolean[noOfColumnStore];

    for (int i = 0; i < columnGroups.length; i++) {
      columnSplit[i] = columnGroups[i].length;
      columnarStore[i] = columnGroups[i].length > 1 ? false : true;
    }
    ColumnGroupModel colGroupModel = new ColumnGroupModel();
    colGroupModel.setNoOfColumnStore(noOfColumnStore);
    colGroupModel.setColumnSplit(columnSplit);
    colGroupModel.setColumnGroupCardinality(dimLens);
    colGroupModel.setColumnarStore(columnarStore);
    colGroupModel.setColumnGroup(columnGroups);
    return colGroupModel;
  }

  /**
   * This method will be used to update the dimension cardinality
   *
   * @param dimCardinality
   * @return new increment cardinality
   */
  public static int[] getIncrementedCardinalityFullyFilled(int[] dimCardinality) {
    int[] newDimsC = new int[dimCardinality.length];
    // get the log bits of cardinality
    for (int i = 0; i < dimCardinality.length; i++) {
      if (dimCardinality[i] == 0) {
        //Array or struct type may have higher value
        newDimsC[i] = 64;
      } else {
        int bitsLength = Long.toBinaryString(dimCardinality[i]).length();
        int div = bitsLength / 8;
        int mod = bitsLength % 8;
        if (mod > 0) {
          newDimsC[i] = 8 * (div + 1);
        } else {
          newDimsC[i] = bitsLength;
        }
      }
    }
    return newDimsC;
  }

  private static int getBitLengthFullyFilled(int dimlens) {
    int bitsLength = Long.toBinaryString(dimlens).length();
    int div = bitsLength / 8;
    int mod = bitsLength % 8;
    if (mod > 0) {
      return 8 * (div + 1);
    } else {
      return bitsLength;
    }
  }

  /**
   * This method will be used to delete the folder and files
   *
   * @param path file path array
   * @throws Exception exception
   */
  public static void deleteFoldersAndFiles(final String... path) throws CarbonUtilException {
    if (path == null) {
      return;
    }
    try {
      UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {

        @Override public Void run() throws Exception {
          for (int i = 0; i < path.length; i++) {
            if (null != path[i]) {
              deleteRecursive(new File(path[i]));
            }
          }
          return null;
        }
      });
    } catch (IOException e) {
      throw new CarbonUtilException("Error while deleteing the folders and files");
    } catch (InterruptedException e) {
      throw new CarbonUtilException("Error while deleteing the folders and files");
    }
  }

  /**
   * This method will be used to delete the folder and files
   *
   * @param path file path array
   * @throws Exception exception
   */
  public static void deleteFoldersAndFiles(final File... path) throws CarbonUtilException {
    try {
      UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {

        @Override public Void run() throws Exception {
          for (int i = 0; i < path.length; i++) {
            deleteRecursive(path[i]);
          }
          return null;
        }
      });
    } catch (IOException e) {
      throw new CarbonUtilException("Error while deleteing the folders and files");
    } catch (InterruptedException e) {
      throw new CarbonUtilException("Error while deleteing the folders and files");
    }

  }

  /**
   * Recursively delete the files
   *
   * @param f File to be deleted
   * @throws CarbonUtilException
   */
  private static void deleteRecursive(File f) throws CarbonUtilException {
    if (f.isDirectory()) {
      if (f.listFiles() != null) {
        for (File c : f.listFiles()) {
          deleteRecursive(c);
        }
      }
    }
    if (f.exists() && !f.delete()) {
      throw new CarbonUtilException("Error while deleteing the folders and files");
    }
  }

  public static void deleteFoldersAndFiles(final CarbonFile... file) throws CarbonUtilException {
    try {
      UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {

        @Override public Void run() throws Exception {
          for (int i = 0; i < file.length; i++) {
            deleteRecursive(file[i]);
          }
          return null;
        }
      });
    } catch (IOException e) {
      throw new CarbonUtilException("Error while deleteing the folders and files");
    } catch (InterruptedException e) {
      throw new CarbonUtilException("Error while deleteing the folders and files");
    }
  }

  public static void deleteFoldersAndFilesSilent(final CarbonFile... file)
      throws CarbonUtilException {
    try {
      UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {

        @Override public Void run() throws Exception {
          for (int i = 0; i < file.length; i++) {
            deleteRecursiveSilent(file[i]);
          }
          return null;
        }
      });
    } catch (IOException e) {
      throw new CarbonUtilException("Error while deleteing the folders and files");
    } catch (InterruptedException e) {
      throw new CarbonUtilException("Error while deleteing the folders and files");
    }
  }

  /**
   * This function will rename the cube to be deleted
   *
   * @param partitionCount
   * @param storePath
   * @param schemaName
   * @param cubeName
   */
  public static void renameCubeForDeletion(int partitionCount, String storePath, String schemaName,
      String cubeName) {
    String cubeNameWithPartition = "";
    String schemaNameWithPartition = "";
    String fullPath = "";
    String newFilePath = "";
    String newFileName = "";
    Callable<Void> c = null;
    long time = System.currentTimeMillis();
    FileFactory.FileType fileType = null;
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    for (int i = 0; i < partitionCount; i++) {
      schemaNameWithPartition = schemaName + '_' + i;
      cubeNameWithPartition = cubeName + '_' + i;
      newFileName = cubeNameWithPartition + '_' + time;
      fullPath = storePath + File.separator + schemaNameWithPartition + File.separator
          + cubeNameWithPartition;
      newFilePath =
          storePath + File.separator + schemaNameWithPartition + File.separator + newFileName;
      fileType = FileFactory.getFileType(fullPath);
      try {
        if (FileFactory.isFileExist(fullPath, fileType)) {
          CarbonFile file = FileFactory.getCarbonFile(fullPath, fileType);
          boolean isRenameSuccessfull = file.renameTo(newFilePath);
          if (!isRenameSuccessfull) {
            LOGGER.error("Problem renaming the cube :: " + fullPath);
            c = new DeleteCube(file);
            executorService.submit(c);
          } else {
            c = new DeleteCube(FileFactory.getCarbonFile(newFilePath, fileType));
            executorService.submit(c);
          }
        }
      } catch (IOException e) {
        LOGGER.error("Problem renaming the cube :: " + fullPath);
      }
    }
    executorService.shutdown();
  }

  /**
   * Recursively delete the files
   *
   * @param f File to be deleted
   * @throws CarbonUtilException
   */
  private static void deleteRecursive(CarbonFile f) throws CarbonUtilException {
    if (f.isDirectory()) {
      if (f.listFiles() != null) {
        for (CarbonFile c : f.listFiles()) {
          deleteRecursive(c);
        }
      }
    }
    if (f.exists() && !f.delete()) {
      throw new CarbonUtilException("Error while deleteing the folders and files");
    }
  }

  private static void deleteRecursiveSilent(CarbonFile f) throws CarbonUtilException {
    if (f.isDirectory()) {
      if (f.listFiles() != null) {
        for (CarbonFile c : f.listFiles()) {
          deleteRecursiveSilent(c);
        }
      }
    }
    if (f.exists() && !f.delete()) {
      return;
    }
  }

  /**
   * This method will be used to read leaf meta data format of meta data will be
   * <entrycount><keylength><keyoffset><measure1length><measure1offset>
   *
   * @param file
   * @param measureCount
   * @param mdKeySize
   * @return will return blocklet info which will have all the meta data
   * related to data file
   */
  public static List<BlockletInfo> getBlockletInfo(File file, int measureCount, int mdKeySize) {
    List<BlockletInfo> listOfBlockletInfo =
        new ArrayList<BlockletInfo>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    String filesLocation = file.getAbsolutePath();
    long fileSize = file.length();
    return getBlockletDetails(listOfBlockletInfo, filesLocation, measureCount, mdKeySize, fileSize);
  }

  /**
   * This method will be used to read leaf meta data format of meta data will be
   * <entrycount><keylength><keyoffset><measure1length><measure1offset>
   *
   * @param file
   * @param measureCount
   * @param mdKeySize
   * @return will return blocklet info which will have all the meta data
   * related to data file
   */
  public static List<BlockletInfo> getBlockletInfo(CarbonFile file, int measureCount,
      int mdKeySize) {
    List<BlockletInfo> listOfNodeInfo =
        new ArrayList<BlockletInfo>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    String filesLocation = file.getAbsolutePath();
    long fileSize = file.getSize();
    return getBlockletDetails(listOfNodeInfo, filesLocation, measureCount, mdKeySize, fileSize);
  }

  /**
   * @param listOfNodeInfo
   * @param filesLocation
   * @param measureCount
   * @param mdKeySize
   * @param fileSize
   * @return
   */
  private static List<BlockletInfo> getBlockletDetails(List<BlockletInfo> listOfNodeInfo,
      String filesLocation, int measureCount, int mdKeySize, long fileSize) {
    long offset = fileSize - CarbonCommonConstants.LONG_SIZE_IN_BYTE;
    FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(filesLocation));
    offset = fileHolder.readDouble(filesLocation, offset);
    int totalMetaDataLength = (int) (fileSize - CarbonCommonConstants.LONG_SIZE_IN_BYTE - offset);
    ByteBuffer buffer =
        ByteBuffer.wrap(fileHolder.readByteArray(filesLocation, offset, totalMetaDataLength));
    buffer.rewind();
    while (buffer.hasRemaining()) {
      int[] msrLength = new int[measureCount];
      long[] msrOffset = new long[measureCount];
      BlockletInfo info = new BlockletInfo();
      byte[] startKey = new byte[mdKeySize];
      byte[] endKey = new byte[mdKeySize];
      info.setFileName(filesLocation);
      info.setNumberOfKeys(buffer.getInt());
      info.setKeyLength(buffer.getInt());
      info.setKeyOffset(buffer.getLong());
      buffer.get(startKey);
      buffer.get(endKey);
      info.setStartKey(startKey);
      info.setEndKey(endKey);
      for (int i = 0; i < measureCount; i++) {
        msrLength[i] = buffer.getInt();
        msrOffset[i] = buffer.getLong();
      }
      info.setMeasureLength(msrLength);
      info.setMeasureOffset(msrOffset);
      listOfNodeInfo.add(info);
    }
    fileHolder.finish();
    return listOfNodeInfo;
  }

  /**
   * This method will be used to read blocklet meta data format of meta data will
   * be <entrycount><keylength><keyoffset><measure1length><measure1offset>
   *
   * @param file
   * @return will return blocklet info which will have all the meta data
   * related to leaf file
   */
  public static List<BlockletInfoColumnar> getBlockletInfoColumnar(CarbonFile file) {
    List<BlockletInfoColumnar> listOfBlockletInfo =
        new ArrayList<BlockletInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    String filesLocation = file.getAbsolutePath();
    long fileSize = file.getSize();
    return getBlockletInfo(listOfBlockletInfo, filesLocation, fileSize);
  }

  /**
   * @param listOfBlockletInfo
   * @param filesLocation
   * @param fileSize
   * @return
   */
  private static List<BlockletInfoColumnar> getBlockletInfo(
      List<BlockletInfoColumnar> listOfBlockletInfo, String filesLocation, long fileSize) {
    long offset = fileSize - CarbonCommonConstants.LONG_SIZE_IN_BYTE;
    FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(filesLocation));
    offset = fileHolder.readDouble(filesLocation, offset);
    CarbonFooterReader metaDataReader = new CarbonFooterReader(filesLocation, offset);
    try {
      listOfBlockletInfo = CarbonMetadataUtil.convertBlockletInfo(metaDataReader.readFooter());
    } catch (IOException e) {
      LOGGER.error("Problem while reading metadata :: " + filesLocation);
    }
    for (BlockletInfoColumnar infoColumnar : listOfBlockletInfo) {
      infoColumnar.setFileName(filesLocation);
    }
    return listOfBlockletInfo;
  }

  /**
   * This method will be used to read the slice metadata
   *
   * @param rsFiles
   * @return slice meta data
   * @throws CarbonUtilException
   */
  public static SliceMetaData readSliceMetadata(File rsFiles, int restructFolderNumber)
      throws CarbonUtilException {
    SliceMetaData readObject = null;
    InputStream stream = null;
    ObjectInputStream objectInputStream = null;
    File file = null;
    try {
      file = new File(rsFiles + File.separator + getSliceMetaDataFileName(restructFolderNumber));
      stream = new FileInputStream(
          rsFiles + File.separator + getSliceMetaDataFileName(restructFolderNumber));
      objectInputStream = new ObjectInputStream(stream);
      readObject = (SliceMetaData) objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new CarbonUtilException(
          "Problem while reading the slicemeta data file " + file.getAbsolutePath(), e);
    }
    //
    catch (IOException e) {
      throw new CarbonUtilException("Problem while reading the slicemeta data file ", e);
    } finally {
      closeStreams(objectInputStream, stream);
    }
    return readObject;
  }

  public static void writeSliceMetaDataFile(String path, SliceMetaData sliceMetaData,
      int nextRestructFolder) {
    OutputStream stream = null;
    ObjectOutputStream objectOutputStream = null;
    try {
      LOGGER.info("Slice Metadata file Path: " + path + '/' + CarbonUtil
          .getSliceMetaDataFileName(nextRestructFolder));
      stream = FileFactory
          .getDataOutputStream(path + File.separator + getSliceMetaDataFileName(nextRestructFolder),
              FileFactory.getFileType(path));
      objectOutputStream = new ObjectOutputStream(stream);
      objectOutputStream.writeObject(sliceMetaData);
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    } finally {
      closeStreams(objectOutputStream, stream);
    }
  }

  public static void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
    for (int i = 0; i < intermediateFiles.length; i++) {
      if (!intermediateFiles[i].delete()) {
        throw new CarbonUtilException("Problem while deleting intermediate file");
      }
    }
  }

  public static ColumnarKeyStoreInfo getColumnarKeyStoreInfo(BlockletInfoColumnar blockletInfo,
      int[] eachBlockSize, ColumnGroupModel colGrpModel) {
    ColumnarKeyStoreInfo columnarKeyStoreInfo = new ColumnarKeyStoreInfo();
    columnarKeyStoreInfo.setFilePath(blockletInfo.getFileName());
    columnarKeyStoreInfo.setIsSorted(blockletInfo.getIsSortedKeyColumn());
    columnarKeyStoreInfo.setKeyBlockIndexLength(blockletInfo.getKeyBlockIndexLength());
    columnarKeyStoreInfo.setKeyBlockIndexOffsets(blockletInfo.getKeyBlockIndexOffSets());
    columnarKeyStoreInfo.setKeyBlockLengths(blockletInfo.getKeyLengths());
    columnarKeyStoreInfo.setKeyBlockOffsets(blockletInfo.getKeyOffSets());
    columnarKeyStoreInfo.setNumberOfKeys(blockletInfo.getNumberOfKeys());
    columnarKeyStoreInfo.setSizeOfEachBlock(eachBlockSize);
    columnarKeyStoreInfo.setNumberCompressor(new NumberCompressor(Integer.parseInt(
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL))));
    columnarKeyStoreInfo.setAggKeyBlock(blockletInfo.getAggKeyBlock());
    columnarKeyStoreInfo.setDataIndexMapLength(blockletInfo.getDataIndexMapLength());
    columnarKeyStoreInfo.setDataIndexMapOffsets(blockletInfo.getDataIndexMapOffsets());
    columnarKeyStoreInfo.setHybridStoreModel(colGrpModel);
    return columnarKeyStoreInfo;
  }

  public static byte[] getKeyArray(ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolder,
      int totalKeySize, int eachKeySize) {
    byte[] completeKeyArray = new byte[totalKeySize];
    byte[] keyBlockData = null;
    int destinationPosition = 0;
    int[] columnIndex = null;
    int blockKeySize = 0;
    for (int i = 0; i < columnarKeyStoreDataHolder.length; i++) {
      keyBlockData = columnarKeyStoreDataHolder[i].getKeyBlockData();
      blockKeySize = columnarKeyStoreDataHolder[i].getColumnarKeyStoreMetadata().getEachRowSize();
      if (columnarKeyStoreDataHolder[i].getColumnarKeyStoreMetadata().isSorted()) {
        for (int j = 0; j < keyBlockData.length; j += blockKeySize) {
          System.arraycopy(keyBlockData, j, completeKeyArray, destinationPosition, blockKeySize);
          destinationPosition += eachKeySize;
        }
      } else {
        columnIndex = columnarKeyStoreDataHolder[i].getColumnarKeyStoreMetadata().getColumnIndex();

        for (int j = 0; j < columnIndex.length; j++) {
          System.arraycopy(keyBlockData, columnIndex[j] * blockKeySize, completeKeyArray,
              eachKeySize * columnIndex[j] + destinationPosition, blockKeySize);
        }
      }
      destinationPosition = blockKeySize;
    }
    return completeKeyArray;
  }

  public static byte[] getKeyArray(ColumnarKeyStoreDataHolder[] columnarKeyStoreDataHolder,
      int totalKeySize, int eachKeySize, short[] columnIndex) {
    byte[] completeKeyArray = new byte[totalKeySize];
    byte[] keyBlockData = null;
    int destinationPosition = 0;
    int blockKeySize = 0;
    for (int i = 0; i < columnarKeyStoreDataHolder.length; i++) {
      keyBlockData = columnarKeyStoreDataHolder[i].getKeyBlockData();
      blockKeySize = columnarKeyStoreDataHolder[i].getColumnarKeyStoreMetadata().getEachRowSize();

      for (int j = 0; j < columnIndex.length; j++) {
        System.arraycopy(keyBlockData, columnIndex[j] * blockKeySize, completeKeyArray,
            destinationPosition, blockKeySize);
        destinationPosition += eachKeySize;
      }
      destinationPosition = blockKeySize;
    }
    return completeKeyArray;
  }

  public static int getFirstIndexUsingBinarySearch(FixedLengthDimensionDataChunk dimColumnDataChunk,
      int low, int high, byte[] compareValue) {
    int cmpResult = 0;
    while (high >= low) {
      int mid = (low + high) / 2;
      cmpResult = ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(dimColumnDataChunk.getCompleteDataChunk(), mid * compareValue.length,
              compareValue.length, compareValue, 0, compareValue.length);
      if (cmpResult < 0) {
        low = mid + 1;
      } else if (cmpResult > 0) {
        high = mid - 1;
      } else {
        int currentIndex = mid;
        while (currentIndex - 1 >= 0 && ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(dimColumnDataChunk.getCompleteDataChunk(),
                (currentIndex - 1) * compareValue.length, compareValue.length, compareValue, 0,
                compareValue.length) == 0) {
          --currentIndex;
        }
        return currentIndex;
      }
    }
    return -1;
  }

  public static int[] getUnCompressColumnIndex(int totalLength, byte[] columnIndexData,
      NumberCompressor numberCompressor) {
    byte[] indexData = null;
    byte[] indexMap = null;
    try {
      ByteBuffer buffer = ByteBuffer.wrap(columnIndexData);
      buffer.rewind();
      int indexDataLength = buffer.getInt();
      indexData = new byte[indexDataLength];
      indexMap = new byte[totalLength - indexDataLength - CarbonCommonConstants.INT_SIZE_IN_BYTE];
      buffer.get(indexData);
      buffer.get(indexMap);
    } catch (Exception e) {
      LOGGER.error("Error while compressColumn Index ");
    }
    return UnBlockIndexer.uncompressIndex(numberCompressor.unCompress(indexData),
        numberCompressor.unCompress(indexMap));
  }

  /**
   * Convert int array to Integer list
   *
   * @param array
   * @return List<Integer>
   */
  public static List<Integer> convertToIntegerList(int[] array) {
    List<Integer> integers = new ArrayList<Integer>();
    for (int i = 0; i < array.length; i++) {
      integers.add(array[i]);
    }
    return integers;
  }

  public static String[] getSlices(String storeLocation,
      FileFactory.FileType fileType) {
    try {
      if (!FileFactory.isFileExist(storeLocation, fileType)) {
        return new String[0];
      }
    } catch (IOException e) {
      LOGGER.error("Error occurred :: " + e.getMessage());
    }
    CarbonFile file = FileFactory.getCarbonFile(storeLocation, fileType);
    CarbonFile[] listFiles = listFiles(file);
    if (null == listFiles || listFiles.length < 0) {
      return new String[0];
    }
    Arrays.sort(listFiles, new CarbonFileFolderComparator());
    String[] slices = new String[listFiles.length];
    for (int i = 0; i < listFiles.length; i++) {
      slices[i] = listFiles[i].getAbsolutePath();
    }
    return slices;
  }

  /**
   * @param file
   * @return
   */
  public static CarbonFile[] listFiles(CarbonFile file) {
    CarbonFile[] listFiles = file.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile pathname) {
        return pathname.getName().startsWith(CarbonCommonConstants.LOAD_FOLDER) && !pathname
            .getName().endsWith(CarbonCommonConstants.FILE_INPROGRESS_STATUS);
      }
    });
    return listFiles;
  }

  public static List<CarbonSliceAndFiles> getSliceAndFilesList(String tableName,
      CarbonFile[] listFiles, FileFactory.FileType fileType) {

    List<CarbonSliceAndFiles> sliceFactFilesList =
        new ArrayList<CarbonSliceAndFiles>(listFiles.length);
    if (listFiles.length == 0) {
      return sliceFactFilesList;
    }

    CarbonSliceAndFiles sliceAndFiles = null;
    CarbonFile[] sortedPathForFiles = null;
    for (int i = 0; i < listFiles.length; i++) {
      sliceAndFiles = new CarbonSliceAndFiles();
      sliceAndFiles.setPath(listFiles[i].getAbsolutePath());
      sortedPathForFiles = getAllFactFiles(sliceAndFiles.getPath(), tableName, fileType);
      if (null != sortedPathForFiles && sortedPathForFiles.length > 0) {
        Arrays.sort(sortedPathForFiles,
            new CarbonFileComparator("\\" + CarbonCommonConstants.FACT_FILE_EXT));
        sliceAndFiles.setSliceFactFilesList(sortedPathForFiles);
        sliceFactFilesList.add(sliceAndFiles);
      }
    }
    return sliceFactFilesList;
  }

  /**
   * Below method will be used to get the fact file present in slice
   *
   * @param sliceLocation slice location
   * @return fact files array
   */
  public static CarbonFile[] getAllFactFiles(String sliceLocation, final String tableName,
      FileFactory.FileType fileType) {
    CarbonFile file = FileFactory.getCarbonFile(sliceLocation, fileType);
    CarbonFile[] files = null;
    CarbonFile[] updatedFactFiles = null;
    if (file.isDirectory()) {
      updatedFactFiles = file.listFiles(new CarbonFileFilter() {

        @Override public boolean accept(CarbonFile pathname) {
          return ((!pathname.isDirectory()) && (pathname.getName().startsWith(tableName))
              && pathname.getName().endsWith(CarbonCommonConstants.FACT_UPDATE_EXTENSION));
        }
      });

      if (updatedFactFiles.length != 0) {
        return updatedFactFiles;

      }

      files = file.listFiles(new CarbonFileFilter() {
        public boolean accept(CarbonFile pathname) {
          return ((!pathname.isDirectory()) && (pathname.getName().startsWith(tableName))
              && pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT));

        }
      });
    }
    return files;
  }

  /**
   * Read level metadata file and return cardinality
   *
   * @param levelPath
   * @return
   * @throws CarbonUtilException
   */
  public static int[] getCardinalityFromLevelMetadataFile(String levelPath)
      throws CarbonUtilException {
    DataInputStream dataInputStream = null;
    int[] cardinality = null;

    try {
      if (FileFactory.isFileExist(levelPath, FileFactory.getFileType(levelPath))) {
        dataInputStream =
            FileFactory.getDataInputStream(levelPath, FileFactory.getFileType(levelPath));

        cardinality = new int[dataInputStream.readInt()];

        for (int i = 0; i < cardinality.length; i++) {
          cardinality[i] = dataInputStream.readInt();
        }
      }
    } catch (FileNotFoundException e) {
      throw new CarbonUtilException("Problem while getting the file", e);
    } catch (IOException e) {
      throw new CarbonUtilException("Problem while reading the file", e);
    } finally {
      closeStreams(dataInputStream);
    }

    return cardinality;
  }

  public static String getNewAggregateTableName(List<String> tableList, String factTableName) {
    int count = 1;
    List<String> newTableList = new ArrayList<String>(10);
    newTableList.addAll(tableList);
    if (newTableList.contains(factTableName)) {
      newTableList.remove(factTableName);
    }
    if (!newTableList.isEmpty()) {
      Collections.sort(newTableList, new AggTableComparator());
      String highestCountAggTableName = newTableList.get(0);
      count = Integer.parseInt(
          highestCountAggTableName.substring(highestCountAggTableName.lastIndexOf("_") + 1))
          + count;
    }
    return CarbonCommonConstants.AGGREGATE_TABLE_START_TAG + CarbonCommonConstants.UNDERSCORE
        + factTableName + CarbonCommonConstants.UNDERSCORE + count;
  }

  public static String getRSPath(String schemaName, String cubeName, String tableName,
      String hdfsLocation, int currentRestructNumber) {
    if (null == hdfsLocation) {
      hdfsLocation =
          CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS);
    }

    String hdfsStoreLocation = hdfsLocation;
    hdfsStoreLocation = hdfsStoreLocation + File.separator + schemaName + File.separator + cubeName;

    int rsCounter = currentRestructNumber/*CarbonUtil.checkAndReturnNextRestructFolderNumber(
                hdfsStoreLocation, "RS_")*/;
    if (rsCounter == -1) {
      rsCounter = 0;
    }
    String hdfsLoadedTable =
        hdfsStoreLocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER + rsCounter
            + "/" + tableName;
    return hdfsLoadedTable;
  }

  /**
   * This method reads the load metadata file
   *
   * @param cubeFolderPath
   * @return
   */
  public static LoadMetadataDetails[] readLoadMetadata(String cubeFolderPath) {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    String metadataFileName = cubeFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.LOADMETADATA_FILENAME;
    LoadMetadataDetails[] listOfLoadFolderDetailsArray;

    AtomicFileOperations fileOperation =
        new AtomicFileOperationsImpl(metadataFileName, FileFactory.getFileType(metadataFileName));

    try {
      if (!FileFactory.isFileExist(metadataFileName, FileFactory.getFileType(metadataFileName))) {
        return new LoadMetadataDetails[0];
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
          CarbonCommonConstants.CARBON_DEFAULT_STREAM_ENCODEFORMAT);
      buffReader = new BufferedReader(inStream);
      listOfLoadFolderDetailsArray =
          gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
    } catch (IOException e) {
      return new LoadMetadataDetails[0];
    } finally {
      closeStreams(buffReader, inStream, dataInputStream);
    }

    return listOfLoadFolderDetailsArray;
  }

  public static boolean createRSMetaFile(String metaDataPath, String newRSFileName) {
    String fullFileName = metaDataPath + File.separator + newRSFileName;
    FileFactory.FileType fileType =
        FileFactory.getFileType(metaDataPath + File.separator + newRSFileName);
    try {
      return FileFactory.createNewFile(fullFileName, fileType);
    } catch (IOException e) {
      LOGGER.error("Error while writing RS meta file : " + fullFileName + e.getMessage());
      return false;
    }
  }

  public static String getSliceMetaDataFileName(int restructFolderNumber) {
    return CarbonCommonConstants.SLICE_METADATA_FILENAME + "." + restructFolderNumber;
  }

  public static void writeLevelCardinalityFile(String loadFolderLoc, String tableName,
      int[] dimCardinality) throws KettleException {
    String levelCardinalityFilePath = loadFolderLoc + File.separator +
        CarbonCommonConstants.LEVEL_METADATA_FILE + tableName
        + CarbonCommonConstants.CARBON_METADATA_EXTENSION;
    FileOutputStream fileOutputStream = null;
    FileChannel channel = null;
    try {
      int dimCardinalityArrLength = dimCardinality.length;

      // first four bytes for writing the length of array, remaining for array data
      ByteBuffer buffer = ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE
          + dimCardinalityArrLength * CarbonCommonConstants.INT_SIZE_IN_BYTE);

      fileOutputStream = new FileOutputStream(levelCardinalityFilePath);
      channel = fileOutputStream.getChannel();
      buffer.putInt(dimCardinalityArrLength);

      for (int i = 0; i < dimCardinalityArrLength; i++) {
        buffer.putInt(dimCardinality[i]);
      }

      buffer.flip();
      channel.write(buffer);
      buffer.clear();

      LOGGER.info("Level cardinality file written to : " + levelCardinalityFilePath);
    } catch (IOException e) {
      LOGGER.error("Error while writing level cardinality file : " + levelCardinalityFilePath + e
          .getMessage());
      throw new KettleException("Not able to write level cardinality file", e);
    } finally {
      closeStreams(channel, fileOutputStream);
    }
  }

  public static SliceMetaData readSliceMetaDataFile(String path) {
    SliceMetaData readObject = null;
    InputStream stream = null;
    ObjectInputStream objectInputStream = null;
    //
    try {
      stream = FileFactory.getDataInputStream(path, FileFactory.getFileType(path));
      objectInputStream = new ObjectInputStream(stream);
      readObject = (SliceMetaData) objectInputStream.readObject();
    } catch (ClassNotFoundException e) {
      LOGGER.error(e);
    } catch (FileNotFoundException e) {
      LOGGER.error("@@@@@ SliceMetaData File is missing @@@@@ :" + path);
    } catch (IOException e) {
      LOGGER.error("@@@@@ Error while reading SliceMetaData File @@@@@ :" + path);
    } finally {
      closeStreams(objectInputStream, stream);
    }
    return readObject;
  }

  public static SliceMetaData readSliceMetaDataFile(String folderPath, int currentRestructNumber) {
    String path = folderPath + '/' + getSliceMetaDataFileName(currentRestructNumber);
    return readSliceMetaDataFile(path);
  }

  /**
   * From beeline if a delimeter is passed as \001, in code we get it as
   * escaped string as \\001. So this method will unescape the slash again and
   * convert it back t0 \001
   *
   * @param parseStr
   * @return
   */
  public static String unescapeChar(String parseStr) {
    switch (parseStr) {
      case "\\001":
        return "\001";
      case "\\t":
        return "\t";
      case "\\r":
        return "\r";
      case "\\b":
        return "\b";
      case "\\f":
        return "\f";
      case "\\n":
        return "\n";
      default:
        return parseStr;
    }
  }

  public static String escapeComplexDelimiterChar(String parseStr) {
    switch (parseStr) {
      case "$":
        return "\\$";
      case ":":
        return "\\:";
      default:
        return parseStr;
    }
  }

  /**
   * Append HDFS Base Url for show create & load data sql
   *
   * @param filePath
   */
  public static String checkAndAppendHDFSUrl(String filePath) {
    String currentPath = filePath;
    if (null != filePath && filePath.length() != 0 &&
        FileFactory.getFileType(filePath) != FileFactory.FileType.HDFS) {
      String baseHDFSUrl = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_DDL_BASE_HDFS_URL);
      String hdfsUrl = conf.get(FS_DEFAULT_FS);
      if (hdfsUrl.startsWith(HDFS_PREFIX)) {
        baseHDFSUrl = hdfsUrl + baseHDFSUrl;
      }
      if (null != baseHDFSUrl) {
        if (baseHDFSUrl.endsWith("/")) {
          baseHDFSUrl = baseHDFSUrl.substring(0, baseHDFSUrl.length() - 1);
        }
        if (!filePath.startsWith("/")) {
          filePath = "/" + filePath;
        }
        currentPath = baseHDFSUrl + filePath;
      }
    }
    return currentPath;
  }

  /**
   * @param location
   * @param factTableName
   * @return
   */
  public static int getRestructureNumber(String location, String factTableName) {
    String restructName =
        location.substring(location.indexOf(CarbonCommonConstants.RESTRUCTRE_FOLDER));
    int factTableIndex = restructName.indexOf(factTableName) - 1;
    String restructNumber =
        restructName.substring(CarbonCommonConstants.RESTRUCTRE_FOLDER.length(), factTableIndex);
    return Integer.parseInt(restructNumber);
  }

  /**
   * This method will read the retry time interval for loading level files in
   * memory
   *
   * @return
   */
  public static long getRetryIntervalForLoadingLevelFile() {
    long retryInterval = 0;
    try {
      retryInterval = Long.parseLong(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_LOAD_LEVEL_RETRY_INTERVAL,
              CarbonCommonConstants.CARBON_LOAD_LEVEL_RETRY_INTERVAL_DEFAULT));
    } catch (NumberFormatException e) {
      retryInterval = Long.parseLong(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_LOAD_LEVEL_RETRY_INTERVAL_DEFAULT));
    }
    retryInterval = retryInterval * 1000;
    return retryInterval;
  }

  /**
   * Below method will be used to get the aggregator type
   * CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE will return when value is double measure
   * CarbonCommonConstants.BYTE_VALUE_MEASURE will be returned when value is byte array
   *
   * @param agg
   * @return aggregator type
   */
  public static char getType(String agg) {
    if (CarbonCommonConstants.SUM.equals(agg) || CarbonCommonConstants.COUNT.equals(agg)) {
      return CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE;
    } else {
      return CarbonCommonConstants.BYTE_VALUE_MEASURE;
    }
  }

  public static String getCarbonStorePath(String schemaName, String cubeName) {
    CarbonProperties prop = CarbonProperties.getInstance();
    if (null == prop) {
      return null;
    }
    String basePath = prop.getProperty(CarbonCommonConstants.STORE_LOCATION,
        CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    String useUniquePath = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_UNIFIED_STORE_PATH,
            CarbonCommonConstants.CARBON_UNIFIED_STORE_PATH_DEFAULT);
    if (null != schemaName && !schemaName.isEmpty() && null != cubeName && !cubeName.isEmpty()
        && "true".equals(useUniquePath)) {
      basePath = basePath + File.separator + schemaName + File.separator + cubeName;
    }
    return basePath;
  }

  /**
   * This method will check the existence of a file at a given path
   */
  public static boolean isFileExists(String fileName) {
    try {
      FileFactory.FileType fileType = FileFactory.getFileType(fileName);
      if (FileFactory.isFileExist(fileName, fileType)) {
        return true;
      }
    } catch (IOException e) {
      LOGGER.error("@@@@@@  File not found at a given location @@@@@@ : " + fileName);
    }
    return false;
  }

  /**
   * This method will check and create the given path
   */
  public static boolean checkAndCreateFolder(String path) {
    boolean created = false;
    try {
      FileFactory.FileType fileType = FileFactory.getFileType(path);
      if (FileFactory.isFileExist(path, fileType)) {
        created = true;
      } else {
        created = FileFactory.mkdirs(path, fileType);
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    }
    return created;
  }

  /**
   * This method will return the size of a given file
   */
  public static long getFileSize(String filePath) {
    FileFactory.FileType fileType = FileFactory.getFileType(filePath);
    CarbonFile carbonFile = FileFactory.getCarbonFile(filePath, fileType);
    return carbonFile.getSize();
  }

  /**
   * This method will be used to get bit length of the dimensions based on the
   * dimension partitioner. If partitioner is value is 1 the column
   * cardinality will be incremented in such a way it will fit in byte level.
   * for example if number of bits required to store one column value is 3
   * bits the 8 bit will be assigned to each value of that column.In this way
   * we may waste some bits(maximum 7 bits) If partitioner value is more than
   * 1 then few column are stored together. so cardinality of that group will
   * be incremented to fit in byte level For example: if cardinality for 3
   * columns stored together is [1,1,1] then number of bits required will be
   * [1,1,1] then last value will be incremented and it will become[1,1,6]
   *
   * @param dimCardinality cardinality of each column
   * @param dimPartitioner Partitioner is how column is stored if value is 1 then column
   *                       wise if value is more than 1 then it is in group with other
   *                       column
   * @return number of bits for each column
   * @TODO for row group only last value is incremented problem in this cases
   * in if last column in that group is selected most of the time in
   * filter query Comparison will be more if it incremented uniformly
   * then comparison will be distributed
   */
  public static int[] getDimensionBitLength(int[] dimCardinality, int[] dimPartitioner) {
    int[] bitLength = new int[dimCardinality.length];
    int dimCounter = 0;
    for (int i = 0; i < dimPartitioner.length; i++) {
      if (dimPartitioner[i] == 1) {
        // for columnar store
        // fully filled bits means complete byte or number of bits
        // assigned will be in
        // multiplication of 8
        bitLength[dimCounter] = getBitLengthFullyFilled(dimCardinality[dimCounter]);
        dimCounter++;
      } else {
        // for row store
        int totalSize = 0;
        for (int j = 0; j < dimPartitioner[i]; j++) {
          bitLength[dimCounter] = getIncrementedCardinality(dimCardinality[dimCounter]);
          totalSize += bitLength[dimCounter];
          dimCounter++;
        }
        // below code is to increment in such a way that row group will
        // be stored
        // as byte level
        int mod = totalSize % 8;
        if (mod > 0) {
          bitLength[dimCounter - 1] = bitLength[dimCounter - 1] + (8 - mod);
        }
      }
    }
    return bitLength;
  }

  /**
   * Below method will be used to get the value compression model of the
   * measure data chunk
   *
   * @param measureDataChunkList
   * @return value compression model
   */
  public static ValueCompressionModel getValueCompressionModel(
      List<DataChunk> measureDataChunkList) {
    Object[] maxValue = new Object[measureDataChunkList.size()];
    Object[] minValue = new Object[measureDataChunkList.size()];
    Object[] uniqueValue = new Object[measureDataChunkList.size()];
    int[] decimal = new int[measureDataChunkList.size()];
    char[] type = new char[measureDataChunkList.size()];
    byte[] dataTypeSelected = new byte[measureDataChunkList.size()];

    /**
     * to fill the meta data required for value compression model
     */
    for (int i = 0; i < dataTypeSelected.length; i++) {
      int indexOf = measureDataChunkList.get(i).getEncodingList().indexOf(Encoding.DELTA);
      if (indexOf > -1) {
        ValueEncoderMeta valueEncoderMeta =
            measureDataChunkList.get(i).getValueEncoderMeta().get(indexOf);
        maxValue[i] = valueEncoderMeta.getMaxValue();
        minValue[i] = valueEncoderMeta.getMinValue();
        uniqueValue[i] = valueEncoderMeta.getUniqueValue();
        decimal[i] = valueEncoderMeta.getDecimal();
        type[i] = valueEncoderMeta.getType();
        dataTypeSelected[i] = valueEncoderMeta.getDataTypeSelected();
      }
    }
    MeasureMetaDataModel measureMetadataModel =
        new MeasureMetaDataModel(minValue, maxValue, decimal, dataTypeSelected.length, uniqueValue,
            type, dataTypeSelected);
    return ValueCompressionUtil.getValueCompressionModel(measureMetadataModel);
  }

  /**
   * Below method will be used to check whether particular encoding is present
   * in the dimension or not
   *
   * @param encoding  encoding to search
   * @return if encoding is present in dimension
   */
  public static boolean hasEncoding(List<Encoding> encodings, Encoding encoding) {
    return encodings.contains(encoding);
  }

  /**
   * below method is to check whether data type is present in the data type array
   *
   * @param dataType  data type to be searched
   * @param dataTypes all data types
   * @return if data type is present
   */
  public static boolean hasDataType(DataType dataType, DataType[] dataTypes) {
    for (int i = 0; i < dataTypes.length; i++) {
      if (dataType.equals(dataTypes[i])) {
        return true;
      }
    }
    return false;
  }

  /**
   * Below method will be used to read the data file matadata
   *
   * @param filePath file path
   * @param blockOffset   offset in the file
   * @return Data file metadata instance
   * @throws CarbonUtilException
   */
  public static DataFileFooter readMetadatFile(String filePath, long blockOffset, long blockLength)
      throws CarbonUtilException {
    DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter();
    try {
      return fileFooterConverter.readDataFileFooter(filePath, blockOffset, blockLength);
    } catch (IOException e) {
      throw new CarbonUtilException("Problem while reading the file metadata", e);
    }
  }

  /**
   * Below method will be used to get the surrogate key
   *
   * @param data   actual data
   * @param buffer byte buffer which will be used to convert the data to integer value
   * @return surrogate key
   */
  public static int getSurrogateKey(byte[] data, ByteBuffer buffer) {
    int lenght = 4 - data.length;
    for (int i = 0; i < lenght; i++) {
      buffer.put((byte) 0);
    }
    buffer.put(data);
    buffer.rewind();
    int surrogate = buffer.getInt();
    buffer.clear();
    return surrogate;
  }

  /**
   * Thread to delete the cubes
   *
   * @author m00258959
   */
  private static final class DeleteCube implements Callable<Void> {
    private CarbonFile file;

    private DeleteCube(CarbonFile file) {
      this.file = file;
    }

    @Override public Void call() throws Exception {
      deleteFoldersAndFiles(file);
      return null;
    }

  }

  private static class CarbonFileComparator implements Comparator<CarbonFile> {
    /**
     * File extension
     */
    private String fileExt;

    public CarbonFileComparator(String fileExt) {
      this.fileExt = fileExt;
    }

    @Override public int compare(CarbonFile file1, CarbonFile file2) {
      String firstFileName = file1.getName().split(fileExt)[0];
      String secondFileName = file2.getName().split(fileExt)[0];
      int lastIndexOfO1 = firstFileName.lastIndexOf('_');
      int lastIndexOfO2 = secondFileName.lastIndexOf('_');
      int f1 = 0;
      int f2 = 0;

      try {
        f1 = Integer.parseInt(firstFileName.substring(lastIndexOfO1 + 1));
        f2 = Integer.parseInt(secondFileName.substring(lastIndexOfO2 + 1));
      } catch (NumberFormatException nfe) {
        return -1;
      }
      return (f1 < f2) ? -1 : (f1 == f2 ? 0 : 1);
    }
  }

  /**
   * class to sort aggregate folder list in descending order
   */
  private static class AggTableComparator implements Comparator<String> {
    public int compare(String aggTable1, String aggTable2) {
      int index1 = aggTable1.lastIndexOf(CarbonCommonConstants.UNDERSCORE);
      int index2 = aggTable2.lastIndexOf(CarbonCommonConstants.UNDERSCORE);
      int n1 = Integer.parseInt(aggTable1.substring(index1 + 1));
      int n2 = Integer.parseInt(aggTable2.substring(index2 + 1));
      if (n1 > n2) {
        return -1;
      } else if (n1 < n2) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  /**
   * Below method will be used to get the dimension
   *
   * @param tableDimensionList table dimension list
   * @return boolean array specifying true if dimension is dictionary
   * and false if dimension is not a dictionary column
   */
  public static boolean[] identifyDimensionType(List<CarbonDimension> tableDimensionList) {
    List<Boolean> isDictionaryDimensions = new ArrayList<Boolean>();
    Set<Integer> processedColumnGroup = new HashSet<Integer>();
    for (CarbonDimension carbonDimension : tableDimensionList) {
      if (carbonDimension.isColumnar() && hasEncoding(carbonDimension.getEncoder(),
          Encoding.DICTIONARY)) {
        isDictionaryDimensions.add(true);
      } else if (!carbonDimension.isColumnar()) {
        if (processedColumnGroup.add(carbonDimension.columnGroupId())) {
          isDictionaryDimensions.add(true);
        }
      } else {
        isDictionaryDimensions.add(false);
      }
    }
    boolean[] primitive = ArrayUtils
        .toPrimitive(isDictionaryDimensions.toArray(new Boolean[isDictionaryDimensions.size()]));
    return primitive;
  }

  /**
   * This method will form one single byte [] for all the high card dims.
   * First it will add all the indexes of variable length byte[] and then the
   * actual value
   *
   * @param byteBufferArr
   * @return byte[] key.
   */
  public static byte[] packByteBufferIntoSingleByteArray(ByteBuffer[] byteBufferArr) {
    // for empty array means there is no data to remove dictionary.
    if (null == byteBufferArr || byteBufferArr.length == 0) {
      return null;
    }
    int noOfCol = byteBufferArr.length;
    short offsetLen = (short) (noOfCol * 2);
    int totalBytes = calculateTotalBytes(byteBufferArr) + offsetLen;
    ByteBuffer buffer = ByteBuffer.allocate(totalBytes);
    // writing the offset of the first element.
    buffer.putShort(offsetLen);

    // prepare index for byte []
    for (int index = 0; index < byteBufferArr.length - 1; index++) {
      ByteBuffer individualCol = byteBufferArr[index];
      int noOfBytes = individualCol.capacity();
      buffer.putShort((short) (offsetLen + noOfBytes));
      offsetLen += noOfBytes;
      individualCol.rewind();
    }

    // put actual data.
    for (int index = 0; index < byteBufferArr.length; index++) {
      ByteBuffer individualCol = byteBufferArr[index];
      buffer.put(individualCol.array());
    }

    buffer.rewind();
    return buffer.array();

  }

  /**
   * To calculate the total bytes in byte Buffer[].
   *
   * @param byteBufferArr
   * @return
   */
  private static int calculateTotalBytes(ByteBuffer[] byteBufferArr) {
    int total = 0;
    for (int index = 0; index < byteBufferArr.length; index++) {
      total += byteBufferArr[index].capacity();
    }
    return total;
  }

}

