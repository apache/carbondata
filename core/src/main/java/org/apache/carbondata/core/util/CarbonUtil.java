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

package org.apache.carbondata.core.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.columnar.ColumnGroupModel;
import org.apache.carbondata.core.datastore.columnar.UnBlockIndexer;
import org.apache.carbondata.core.datastore.compression.MeasureMetaDataModel;
import org.apache.carbondata.core.datastore.compression.WriterCompressModel;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.keygenerator.mdkey.NumberCompressor;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.reader.ThriftReader.TBaseCreator;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.service.CarbonCommonFactory;
import org.apache.carbondata.core.service.PathService;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.DataChunk3;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.pentaho.di.core.exception.KettleException;

public final class CarbonUtil {

  public static final String HDFS_PREFIX = "hdfs://";
  public static final String VIEWFS_PREFIX = "viewfs://";
  public static final String ALLUXIO_PREFIX = "alluxio://";
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
        try {
          closeStream(stream);
        } catch (IOException e) {
          LOGGER.error("Error while closing stream:" + e);
        }
      }
    }
  }

  /**
   * This method closes stream
   *
   * @param stream
   * @throws IOException
   */
  public static void closeStream(Closeable stream) throws IOException {
    if (null != stream) {
      stream.close();
    }
  }

  /**
   * This method will be used to update the dimension cardinality
   *
   * @param dimCardinality
   * @return new increment cardinality
   */
  public static int[] getIncrementedCardinality(int[] dimCardinality) {
    // get the cardinality incr factor
    final int incrValue = CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL;

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
    final int incrValue = CarbonCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL;

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
   * @param columnGroups : column groups
   * @return ColumnGroupModel  model
   */
  public static ColumnGroupModel getColGroupModel(int[][] columnGroups) {
    int[] columnSplit = new int[columnGroups.length];
    int noOfColumnStore = columnSplit.length;
    boolean[] columnarStore = new boolean[noOfColumnStore];

    for (int i = 0; i < columnGroups.length; i++) {
      columnSplit[i] = columnGroups[i].length;
      columnarStore[i] = columnGroups[i].length <= 1;
    }
    ColumnGroupModel colGroupModel = new ColumnGroupModel();
    colGroupModel.setNoOfColumnStore(noOfColumnStore);
    colGroupModel.setColumnSplit(columnSplit);
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
  public static void deleteFoldersAndFiles(final File... path)
      throws IOException, InterruptedException {
    UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {

      @Override public Void run() throws Exception {
        for (int i = 0; i < path.length; i++) {
          deleteRecursive(path[i]);
        }
        return null;
      }
    });
  }

  /**
   * Recursively delete the files
   *
   * @param f File to be deleted
   * @throws IOException
   */
  private static void deleteRecursive(File f) throws IOException {
    if (f.isDirectory()) {
      if (f.listFiles() != null) {
        for (File c : f.listFiles()) {
          deleteRecursive(c);
        }
      }
    }
    if (f.exists() && !f.delete()) {
      throw new IOException("Error while deleting the folders and files");
    }
  }

  public static void deleteFoldersAndFiles(final CarbonFile... file)
      throws IOException, InterruptedException {
    UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {

      @Override public Void run() throws Exception {
        for (int i = 0; i < file.length; i++) {
          deleteRecursive(file[i]);
        }
        return null;
      }
    });
  }

  public static String getBadLogPath(String storeLocation) {
    String badLogStoreLocation =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC);
    badLogStoreLocation = badLogStoreLocation + File.separator + storeLocation;

    return badLogStoreLocation;
  }

  public static void deleteFoldersAndFilesSilent(final CarbonFile... file)
      throws IOException, InterruptedException {
    UserGroupInformation.getLoginUser().doAs(new PrivilegedExceptionAction<Void>() {

      @Override public Void run() throws Exception {
        for (int i = 0; i < file.length; i++) {
          deleteRecursiveSilent(file[i]);
        }
        return null;
      }
    });
  }

  /**
   * Recursively delete the files
   *
   * @param f File to be deleted
   * @throws IOException
   */
  private static void deleteRecursive(CarbonFile f) throws IOException {
    if (f.isDirectory()) {
      if (f.listFiles() != null) {
        for (CarbonFile c : f.listFiles()) {
          deleteRecursive(c);
        }
      }
    }
    if (f.exists() && !f.delete()) {
      throw new IOException("Error while deleting the folders and files");
    }
  }

  private static void deleteRecursiveSilent(CarbonFile f) {
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

  public static void deleteFiles(File[] intermediateFiles) throws IOException {
    for (int i = 0; i < intermediateFiles.length; i++) {
      if (!intermediateFiles[i].delete()) {
        throw new IOException("Problem while deleting intermediate file");
      }
    }
  }

  public static int getFirstIndexUsingBinarySearch(DimensionColumnDataChunk dimColumnDataChunk,
      int low, int high, byte[] compareValue, boolean matchUpLimit) {
    int cmpResult = 0;
    while (high >= low) {
      int mid = (low + high) / 2;
      cmpResult = dimColumnDataChunk.compareTo(mid, compareValue);
      if (cmpResult < 0) {
        low = mid + 1;
      } else if (cmpResult > 0) {
        high = mid - 1;
      } else {
        int currentIndex = mid;
        if (!matchUpLimit) {
          while (currentIndex - 1 >= 0
              && dimColumnDataChunk.compareTo(currentIndex - 1, compareValue) == 0) {
            --currentIndex;
          }
        } else {
          while (currentIndex + 1 <= high
              && dimColumnDataChunk.compareTo(currentIndex + 1, compareValue) == 0) {
            currentIndex++;
          }
        }
        return currentIndex;
      }
    }
    return -(low + 1);
  }

  /**
   * search a specific compareValue's range index in a sorted byte array
   *
   * @param dimColumnDataChunk
   * @param low
   * @param high
   * @param compareValue
   * @return the compareValue's range index in the dimColumnDataChunk
   */
  public static int[] getRangeIndexUsingBinarySearch(
      DimensionColumnDataChunk dimColumnDataChunk, int low, int high, byte[] compareValue) {

    int[] rangeIndex = new int[2];
    int cmpResult = 0;
    while (high >= low) {
      int mid = (low + high) / 2;
      cmpResult = dimColumnDataChunk.compareTo(mid, compareValue);
      if (cmpResult < 0) {
        low = mid + 1;
      } else if (cmpResult > 0) {
        high = mid - 1;
      } else {

        int currentIndex = mid;
        while (currentIndex - 1 >= 0
            && dimColumnDataChunk.compareTo(currentIndex - 1, compareValue) == 0) {
          --currentIndex;
        }
        rangeIndex[0] = currentIndex;

        currentIndex = mid;
        while (currentIndex + 1 <= high
            && dimColumnDataChunk.compareTo(currentIndex + 1, compareValue) == 0) {
          currentIndex++;
        }
        rangeIndex[1] = currentIndex;

        return rangeIndex;
      }
    }

    // key not found. return a not exist range
    // rangeIndex[0] = 0;
    rangeIndex[1] = -1;
    return rangeIndex;
  }

  /**
   * Checks that {@code fromIndex} and {@code toIndex} are in the range and
   * throws an exception if they aren't.
   */
  private static void rangeCheck(int fromIndex, int toIndex) {
    if (fromIndex > toIndex) {
      throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
    }
    if (fromIndex < 0) {
      throw new ArrayIndexOutOfBoundsException(fromIndex);
    }
  }

  /**
   * search a specific key in sorted byte array
   *
   * @param filterValues
   * @param low
   * @param high
   * @param compareValue
   * @return the compareValue's index in the filterValues
   */
  public static int binarySearch(byte[][] filterValues, int low, int high,
      byte[] compareValue) {

    rangeCheck(low, high);

    while (low <= high) {
      int mid = (low + high) >>> 1;

      int result = ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[mid], compareValue);

      if (result < 0) {
        low = mid + 1;
      } else if (result > 0) {
        high = mid - 1;
      } else {

        return mid; // key found
      }

    }
    // key not found
    return -(low + 1);
  }


  /**
   * Method will identify the value which is lesser than the pivot element
   * on which range filter is been applied.
   *
   * @param currentIndex
   * @param dimColumnDataChunk
   * @param compareValue
   * @return index value
   */
  public static int nextLesserValueToTarget(int currentIndex,
      DimensionColumnDataChunk dimColumnDataChunk, byte[] compareValue) {
    while (currentIndex - 1 >= 0
        && dimColumnDataChunk.compareTo(currentIndex - 1, compareValue) >= 0) {
      --currentIndex;
    }

    return --currentIndex;
  }

  /**
   * Method will identify the value which is greater than the pivot element
   * on which range filter is been applied.
   *
   * @param currentIndex
   * @param dimColumnDataChunk
   * @param compareValue
   * @param numerOfRows
   * @return index value
   */
  public static int nextGreaterValueToTarget(int currentIndex,
      DimensionColumnDataChunk dimColumnDataChunk, byte[] compareValue, int numerOfRows) {
    while (currentIndex + 1 < numerOfRows
        && dimColumnDataChunk.compareTo(currentIndex + 1, compareValue) <= 0) {
      ++currentIndex;
    }

    return ++currentIndex;
  }

  public static int[] getUnCompressColumnIndex(int totalLength, byte[] columnIndexData,
      NumberCompressor numberCompressor, int offset) {
    ByteBuffer buffer = ByteBuffer.wrap(columnIndexData, offset, totalLength);
    int indexDataLength = buffer.getInt();
    byte[] indexData = new byte[indexDataLength];
    byte[] indexMap =
        new byte[totalLength - indexDataLength - CarbonCommonConstants.INT_SIZE_IN_BYTE];
    buffer.get(indexData);
    buffer.get(indexMap);
    return UnBlockIndexer
        .uncompressIndex(numberCompressor.unCompress(indexData, 0, indexData.length),
            numberCompressor.unCompress(indexMap, 0, indexMap.length));
  }

  public static int[] getUnCompressColumnIndex(int totalLength, ByteBuffer buffer, int offset) {
    buffer.position(offset);
    int indexDataLength = buffer.getInt();
    int indexMapLength = totalLength - indexDataLength - CarbonCommonConstants.INT_SIZE_IN_BYTE;
    int[] indexData = getIntArray(buffer, buffer.position(), indexDataLength);
    int[] indexMap = getIntArray(buffer, buffer.position(), indexMapLength);
    return UnBlockIndexer.uncompressIndex(indexData, indexMap);
  }

  public static int[] getIntArray(ByteBuffer data, int offset, int length) {
    if (length == 0) {
      return new int[0];
    }
    data.position(offset);
    int[] intArray = new int[length / 2];
    int index = 0;
    while (index < intArray.length) {
      intArray[index++] = data.getShort();
    }
    return intArray;
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

  /**
   * Read level metadata file and return cardinality
   *
   * @param levelPath
   * @return
   * @throws IOException
   */
  public static int[] getCardinalityFromLevelMetadataFile(String levelPath) throws IOException {
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
    } finally {
      closeStreams(dataInputStream);
    }

    return cardinality;
  }

  public static void writeLevelCardinalityFile(String loadFolderLoc, String tableName,
      int[] dimCardinality) throws KettleException {
    String levelCardinalityFilePath =
        loadFolderLoc + File.separator + CarbonCommonConstants.LEVEL_METADATA_FILE + tableName
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

  /**
   * From beeline if a delimeter is passed as \001, in code we get it as
   * escaped string as \\001. So this method will unescape the slash again and
   * convert it back t0 \001
   *
   * @param parseStr
   * @return
   */
  public static String unescapeChar(String parseStr) {
    return scala.StringContext.treatEscapes(parseStr);
  }

  /**
   * special char delimiter Converter
   *
   * @param delimiter
   * @return delimiter
   */
  public static String delimiterConverter(String delimiter) {
    switch (delimiter) {
      case "|":
      case "*":
      case ".":
      case ":":
      case "^":
      case "\\":
      case "$":
      case "+":
      case "?":
      case "(":
      case ")":
      case "{":
      case "}":
      case "[":
      case "]":
        return "\\" + delimiter;
      default:
        return delimiter;
    }
  }

  /**
   * Append HDFS Base Url for show create & load data sql
   *
   * @param filePath
   */
  public static String checkAndAppendHDFSUrl(String filePath) {
    String currentPath = filePath;
    if (null != filePath && filePath.length() != 0
        && FileFactory.getFileType(filePath) != FileFactory.FileType.HDFS
        && FileFactory.getFileType(filePath) != FileFactory.FileType.VIEWFS) {
      String baseDFSUrl = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_DDL_BASE_HDFS_URL);
      if (null != baseDFSUrl) {
        String dfsUrl = conf.get(FS_DEFAULT_FS);
        if (null != dfsUrl && (dfsUrl.startsWith(HDFS_PREFIX) || dfsUrl
            .startsWith(VIEWFS_PREFIX))) {
          baseDFSUrl = dfsUrl + baseDFSUrl;
        }
        if (baseDFSUrl.endsWith("/")) {
          baseDFSUrl = baseDFSUrl.substring(0, baseDFSUrl.length() - 1);
        }
        if (!filePath.startsWith("/")) {
          filePath = "/" + filePath;
        }
        currentPath = baseDFSUrl + filePath;
      }
    }
    return currentPath;
  }

  public static String getCarbonStorePath() {
    CarbonProperties prop = CarbonProperties.getInstance();
    if (null == prop) {
      return null;
    }
    String basePath = prop.getProperty(CarbonCommonConstants.STORE_LOCATION,
        CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
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
   * @return value compression model
   */
  public static WriterCompressModel getValueCompressionModel(
      List<ValueEncoderMeta> encodeMetaList) {
    Object[] maxValue = new Object[encodeMetaList.size()];
    Object[] minValue = new Object[encodeMetaList.size()];
    Object[] uniqueValue = new Object[encodeMetaList.size()];
    int[] decimal = new int[encodeMetaList.size()];
    char[] type = new char[encodeMetaList.size()];
    byte[] dataTypeSelected = new byte[encodeMetaList.size()];

    /**
     * to fill the meta data required for value compression model
     */
    for (int i = 0; i < dataTypeSelected.length; i++) {  // always 1
      ValueEncoderMeta valueEncoderMeta = encodeMetaList.get(i);
      maxValue[i] = valueEncoderMeta.getMaxValue();
      minValue[i] = valueEncoderMeta.getMinValue();
      uniqueValue[i] = valueEncoderMeta.getUniqueValue();
      decimal[i] = valueEncoderMeta.getDecimal();
      type[i] = valueEncoderMeta.getType();
      dataTypeSelected[i] = valueEncoderMeta.getDataTypeSelected();
    }
    MeasureMetaDataModel measureMetadataModel =
        new MeasureMetaDataModel(minValue, maxValue, decimal, dataTypeSelected.length, uniqueValue,
            type, dataTypeSelected);
    return ValueCompressionUtil.getWriterCompressModel(measureMetadataModel);
  }

  /**
   * Below method will be used to check whether particular encoding is present
   * in the dimension or not
   *
   * @param encoding encoding to search
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
   * below method is to check whether it is complex data type
   *
   * @param dataType data type to be searched
   * @return if data type is present
   */
  public static boolean hasComplexDataType(DataType dataType) {
    switch (dataType) {
      case ARRAY:
      case STRUCT:
      case MAP:
        return true;
      default:
        return false;
    }
  }

  public static boolean[] getDictionaryEncodingArray(QueryDimension[] queryDimensions) {
    boolean[] dictionaryEncodingArray = new boolean[queryDimensions.length];
    for (int i = 0; i < queryDimensions.length; i++) {
      dictionaryEncodingArray[i] =
          queryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY);
    }
    return dictionaryEncodingArray;
  }

  public static boolean[] getDirectDictionaryEncodingArray(QueryDimension[] queryDimensions) {
    boolean[] dictionaryEncodingArray = new boolean[queryDimensions.length];
    for (int i = 0; i < queryDimensions.length; i++) {
      dictionaryEncodingArray[i] =
          queryDimensions[i].getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY);
    }
    return dictionaryEncodingArray;
  }

  public static boolean[] getImplicitColumnArray(QueryDimension[] queryDimensions) {
    boolean[] implicitColumnArray = new boolean[queryDimensions.length];
    for (int i = 0; i < queryDimensions.length; i++) {
      implicitColumnArray[i] = queryDimensions[i].getDimension().hasEncoding(Encoding.IMPLICIT);
    }
    return implicitColumnArray;
  }

  public static boolean[] getComplexDataTypeArray(QueryDimension[] queryDimensions) {
    boolean[] dictionaryEncodingArray = new boolean[queryDimensions.length];
    for (int i = 0; i < queryDimensions.length; i++) {
      dictionaryEncodingArray[i] =
          CarbonUtil.hasComplexDataType(queryDimensions[i].getDimension().getDataType());
    }
    return dictionaryEncodingArray;
  }

  /**
   * Below method will be used to read the data file matadata
   */
  public static DataFileFooter readMetadatFile(TableBlockInfo tableBlockInfo) throws IOException {
    AbstractDataFileFooterConverter fileFooterConverter =
        DataFileFooterConverterFactory.getInstance()
            .getDataFileFooterConverter(tableBlockInfo.getVersion());
    return fileFooterConverter.readDataFileFooter(tableBlockInfo);
  }

  /**
   * The method calculate the B-Tree metadata size.
   *
   * @param tableBlockInfo
   * @return
   */
  public static long calculateMetaSize(TableBlockInfo tableBlockInfo) throws IOException {
    FileHolder fileReader = null;
    try {
      long completeBlockLength = tableBlockInfo.getBlockLength();
      long footerPointer = completeBlockLength - 8;
      String filePath = tableBlockInfo.getFilePath();
      fileReader = FileFactory.getFileHolder(FileFactory.getFileType(filePath));
      long actualFooterOffset = fileReader.readLong(filePath, footerPointer);
      return footerPointer - actualFooterOffset;
    } finally {
      if (null != fileReader) {
        try {
          fileReader.finish();
        } catch (IOException e) {
          // ignore the exception as nothing we can do about it
        }
      }
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
   * The method returns the B-Tree for a particular taskId
   *
   * @param taskId
   * @param tableBlockInfoList
   * @param absoluteTableIdentifier
   */
  public static long calculateDriverBTreeSize(String taskId, String bucketNumber,
      List<TableBlockInfo> tableBlockInfoList, AbsoluteTableIdentifier absoluteTableIdentifier) {
    // need to sort the  block info list based for task in ascending  order so
    // it will be sinkup with block index read from file
    Collections.sort(tableBlockInfoList);
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());
    // geting the index file path
    //TODO need to pass proper partition number when partiton will be supported
    String carbonIndexFilePath = carbonTablePath
        .getCarbonIndexFilePath(taskId, "0", tableBlockInfoList.get(0).getSegmentId(),
            bucketNumber);
    CarbonFile carbonFile = FileFactory
        .getCarbonFile(carbonIndexFilePath, FileFactory.getFileType(carbonIndexFilePath));
    // in case of carbonIndex file whole file is meta only so reading complete file.
    return carbonFile.getSize();
  }

  /**
   * This method will clear the B-Tree Cache in executors for the given list of blocks
   *
   * @param dataBlocks
   */
  public static void clearBlockCache(List<AbstractIndex> dataBlocks) {
    if (null != dataBlocks) {
      for (AbstractIndex blocks : dataBlocks) {
        blocks.clear();
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
      List<CarbonDimension> childs = carbonDimension.getListOfChildDimensions();
      //assuming complex dimensions will always be atlast
      if (null != childs && childs.size() > 0) {
        break;
      }
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

  /**
   * Find the dimension from metadata by using unique name. As of now we are
   * taking level name as unique name. But user needs to give one unique name
   * for each level,that level he needs to mention in query.
   *
   * @param dimensions
   * @param carbonDim
   * @return
   */
  public static CarbonDimension findDimension(List<CarbonDimension> dimensions, String carbonDim) {
    CarbonDimension findDim = null;
    for (CarbonDimension dimension : dimensions) {
      if (dimension.getColName().equalsIgnoreCase(carbonDim)) {
        findDim = dimension;
        break;
      }
    }
    return findDim;
  }

  /**
   * This method will search for a given dimension in the current block dimensions list
   *
   * @param blockDimensions
   * @param dimensionToBeSearched
   * @return
   */
  public static CarbonDimension getDimensionFromCurrentBlock(
      List<CarbonDimension> blockDimensions, CarbonDimension dimensionToBeSearched) {
    CarbonDimension currentBlockDimension = null;
    for (CarbonDimension blockDimension : blockDimensions) {
      if (dimensionToBeSearched.getColumnId().equals(blockDimension.getColumnId())) {
        currentBlockDimension = blockDimension;
        break;
      }
    }
    return currentBlockDimension;
  }

  /**
   * This method will search for a given measure in the current block measures list
   *
   * @param blockMeasures
   * @param columnId
   * @return
   */
  public static CarbonMeasure getMeasureFromCurrentBlock(List<CarbonMeasure> blockMeasures,
      String columnId) {
    CarbonMeasure currentBlockMeasure = null;
    for (CarbonMeasure blockMeasure : blockMeasures) {
      if (columnId.equals(blockMeasure.getColumnId())) {
        currentBlockMeasure = blockMeasure;
        break;
      }
    }
    return currentBlockMeasure;
  }

  /**
   * This method will be used to clear the dictionary cache after its usage is complete
   * so that if memory threshold is reached it can evicted from LRU cache
   *
   * @param dictionary
   */
  public static void clearDictionaryCache(Dictionary dictionary) {
    if (null != dictionary) {
      dictionary.clear();
    }
  }

  /**
   * @param dictionaryColumnCardinality
   * @param wrapperColumnSchemaList
   * @return It returns formatted cardinality by adding -1 value for NoDictionary columns
   */
  public static int[] getFormattedCardinality(int[] dictionaryColumnCardinality,
      List<ColumnSchema> wrapperColumnSchemaList) {
    List<Integer> cardinality = new ArrayList<>();
    int counter = 0;
    for (int i = 0; i < wrapperColumnSchemaList.size(); i++) {
      if (CarbonUtil.hasEncoding(wrapperColumnSchemaList.get(i).getEncodingList(),
          org.apache.carbondata.core.metadata.encoder.Encoding.DICTIONARY)) {
        cardinality.add(dictionaryColumnCardinality[counter]);
        counter++;
      } else if (!wrapperColumnSchemaList.get(i).isDimensionColumn()) {
        continue;
      } else {
        cardinality.add(-1);
      }
    }
    return ArrayUtils.toPrimitive(cardinality.toArray(new Integer[cardinality.size()]));
  }

  public static List<ColumnSchema> getColumnSchemaList(List<CarbonDimension> carbonDimensionsList,
      List<CarbonMeasure> carbonMeasureList) {
    List<ColumnSchema> wrapperColumnSchemaList = new ArrayList<ColumnSchema>();
    fillCollumnSchemaListForComplexDims(carbonDimensionsList, wrapperColumnSchemaList);
    for (CarbonMeasure carbonMeasure : carbonMeasureList) {
      wrapperColumnSchemaList.add(carbonMeasure.getColumnSchema());
    }
    return wrapperColumnSchemaList;
  }

  private static void fillCollumnSchemaListForComplexDims(
      List<CarbonDimension> carbonDimensionsList, List<ColumnSchema> wrapperColumnSchemaList) {
    for (CarbonDimension carbonDimension : carbonDimensionsList) {
      wrapperColumnSchemaList.add(carbonDimension.getColumnSchema());
      List<CarbonDimension> childDims = carbonDimension.getListOfChildDimensions();
      if (null != childDims && childDims.size() > 0) {
        fillCollumnSchemaListForComplexDims(childDims, wrapperColumnSchemaList);
      }
    }
  }

  /**
   * Below method will be used to get all the block index info from index file
   *
   * @param taskId                  task id of the file
   * @param tableBlockInfoList      list of table block
   * @param absoluteTableIdentifier absolute table identifier
   * @return list of block info
   * @throws IOException if any problem while reading
   */
  public static List<DataFileFooter> readCarbonIndexFile(String taskId, String bucketNumber,
      List<TableBlockInfo> tableBlockInfoList, AbsoluteTableIdentifier absoluteTableIdentifier)
      throws IOException {
    // need to sort the  block info list based for task in ascending  order so
    // it will be sinkup with block index read from file
    Collections.sort(tableBlockInfoList);
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());
    // geting the index file path
    //TODO need to pass proper partition number when partiton will be supported
    String carbonIndexFilePath = carbonTablePath
        .getCarbonIndexFilePath(taskId, "0", tableBlockInfoList.get(0).getSegmentId(),
            bucketNumber);
    DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter();
    // read the index info and return
    return fileFooterConverter.getIndexInfo(carbonIndexFilePath, tableBlockInfoList);
  }

  /**
   * initialize the value of dictionary chunk that can be kept in memory at a time
   *
   * @return
   */
  public static int getDictionaryChunkSize() {
    int dictionaryOneChunkSize = 0;
    try {
      dictionaryOneChunkSize = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE,
              CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE_DEFAULT));
    } catch (NumberFormatException e) {
      dictionaryOneChunkSize =
          Integer.parseInt(CarbonCommonConstants.DICTIONARY_ONE_CHUNK_SIZE_DEFAULT);
      LOGGER.error("Dictionary chunk size not configured properly. Taking default size "
          + dictionaryOneChunkSize);
    }
    return dictionaryOneChunkSize;
  }

  /**
   * @param csvFilePath
   * @return
   */
  public static String readHeader(String csvFilePath) throws IOException {

    DataInputStream fileReader = null;
    BufferedReader bufferedReader = null;
    String readLine = null;

    try {
      fileReader =
          FileFactory.getDataInputStream(csvFilePath, FileFactory.getFileType(csvFilePath));
      bufferedReader = new BufferedReader(new InputStreamReader(fileReader,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));
      readLine = bufferedReader.readLine();
    } finally {
      CarbonUtil.closeStreams(fileReader, bufferedReader);
    }
    return readLine;
  }

  /**
   * Below method will create string like "***********"
   *
   * @param a
   * @param num
   */
  public static String printLine(String a, int num) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < num; i++) {
      builder.append(a);
    }
    return builder.toString();
  }

  /**
   * Below method will be used to get the list of segment in
   * comma separated string format
   *
   * @param segmentList
   * @return comma separated segment string
   */
  public static String getSegmentString(List<String> segmentList) {
    if (segmentList.isEmpty()) {
      return "";
    }
    StringBuilder segmentStringbuilder = new StringBuilder();
    for (int i = 0; i < segmentList.size() - 1; i++) {
      String segmentNo = segmentList.get(i);
      segmentStringbuilder.append(segmentNo);
      segmentStringbuilder.append(",");
    }
    segmentStringbuilder.append(segmentList.get(segmentList.size() - 1));
    return segmentStringbuilder.toString();
  }

  /**
   * Below method will be used to convert the thrift object to byte array.
   */
  public static byte[] getByteArray(TBase t) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    byte[] thriftByteArray = null;
    TProtocol binaryOut = new TCompactProtocol(new TIOStreamTransport(stream));
    try {
      t.write(binaryOut);
      stream.flush();
      thriftByteArray = stream.toByteArray();
    } catch (TException | IOException e) {
      closeStreams(stream);
    } finally {
      closeStreams(stream);
    }
    return thriftByteArray;
  }

  /**
   * Below method will be used to convert the bytearray to data chunk object
   *
   * @param dataChunkBytes datachunk thrift object in bytes
   * @return data chunk thrift object
   */
  public static DataChunk2 readDataChunk(byte[] dataChunkBytes, int offset, int length)
      throws IOException {
    return (DataChunk2) read(dataChunkBytes, new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new DataChunk2();
      }
    }, offset, length);
  }

  public static DataChunk3 readDataChunk3(ByteBuffer dataChunkBuffer, int offset, int length)
      throws IOException {
    byte[] data = new byte[length];
    dataChunkBuffer.position(offset);
    dataChunkBuffer.get(data);
    return (DataChunk3) read(data, new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new DataChunk3();
      }
    }, 0, length);
  }

  public static DataChunk2 readDataChunk(ByteBuffer dataChunkBuffer, int offset, int length)
      throws IOException {
    byte[] data = new byte[length];
    dataChunkBuffer.position(offset);
    dataChunkBuffer.get(data);
    return (DataChunk2) read(data, new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new DataChunk2();
      }
    }, 0, length);
  }

  /**
   * Below method will be used to convert the byte array value to thrift object for
   * data chunk
   *
   * @param data    thrift byte array
   * @param creator type of thrift
   * @return thrift object
   * @throws IOException any problem while converting the object
   */
  private static TBase read(byte[] data, TBaseCreator creator, int offset, int length)
      throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(data, offset, length);
    TProtocol binaryIn = new TCompactProtocol(new TIOStreamTransport(stream));
    TBase t = creator.create();
    try {
      t.read(binaryIn);
    } catch (TException e) {
      throw new IOException(e);
    } finally {
      CarbonUtil.closeStreams(stream);
    }
    return t;
  }

  /**
   * Below method will be used to convert the encode metadata to
   * ValueEncoderMeta object
   *
   * @param encoderMeta
   * @return ValueEncoderMeta object
   */
  public static ValueEncoderMeta deserializeEncoderMeta(byte[] encoderMeta) {
    // TODO : should remove the unnecessary fields.
    ByteArrayInputStream aos = null;
    ObjectInputStream objStream = null;
    ValueEncoderMeta meta = null;
    try {
      aos = new ByteArrayInputStream(encoderMeta);
      objStream = new ObjectInputStream(aos);
      meta = (ValueEncoderMeta) objStream.readObject();
    } catch (ClassNotFoundException e) {
      LOGGER.error(e);
    } catch (IOException e) {
      CarbonUtil.closeStreams(objStream);
    }
    return meta;
  }

  public static ValueEncoderMeta deserializeEncoderMetaNew(byte[] encodeMeta) {
    ByteBuffer buffer = ByteBuffer.wrap(encodeMeta);
    char measureType = buffer.getChar();
    ValueEncoderMeta valueEncoderMeta = new ValueEncoderMeta();
    valueEncoderMeta.setType(measureType);
    switch (measureType) {
      case CarbonCommonConstants.SUM_COUNT_VALUE_MEASURE:
        valueEncoderMeta.setMaxValue(buffer.getDouble());
        valueEncoderMeta.setMinValue(buffer.getDouble());
        valueEncoderMeta.setUniqueValue(buffer.getDouble());
        break;
      case CarbonCommonConstants.BIG_DECIMAL_MEASURE:
        valueEncoderMeta.setMaxValue(0.0);
        valueEncoderMeta.setMinValue(0.0);
        valueEncoderMeta.setUniqueValue(0.0);
        break;
      case CarbonCommonConstants.BIG_INT_MEASURE:
        valueEncoderMeta.setMaxValue(buffer.getLong());
        valueEncoderMeta.setMinValue(buffer.getLong());
        valueEncoderMeta.setUniqueValue(buffer.getLong());
        break;
      default:
        throw new IllegalArgumentException("invalid measure type");
    }
    valueEncoderMeta.setDecimal(buffer.getInt());
    valueEncoderMeta.setDataTypeSelected(buffer.get());
    return valueEncoderMeta;
  }

  /**
   * Below method will be used to convert indexes in range
   * Indexes=[0,1,2,3,4,5,6,7,8,9]
   * Length=9
   * number of element in group =5
   * then output will be [0,1,2,3,4],[5,6,7,8],[9]
   *
   * @param indexes                indexes
   * @param length                 number of element to be considered
   * @param numberOfElementInGroup number of element in group
   * @return range indexes
   */
  public static int[][] getRangeIndex(int[] indexes, int length, int numberOfElementInGroup) {
    List<List<Integer>> rangeList = new ArrayList<>();
    int[][] outputArray = null;
    int k = 0;
    int index = 1;
    if (indexes.length == 1) {
      outputArray = new int[1][2];
      outputArray[0][0] = indexes[0];
      outputArray[0][1] = indexes[0];
      return outputArray;
    }
    while (index < length) {
      if (indexes[index] - indexes[index - 1] == 1 && k < numberOfElementInGroup - 1) {
        k++;
      } else {
        if (k > 0) {
          List<Integer> range = new ArrayList<>();
          rangeList.add(range);
          range.add(indexes[index - k - 1]);
          range.add(indexes[index - 1]);
        } else {
          List<Integer> range = new ArrayList<>();
          rangeList.add(range);
          range.add(indexes[index - 1]);
        }
        k = 0;
      }
      index++;
    }
    if (k > 0) {
      List<Integer> range = new ArrayList<>();
      rangeList.add(range);
      range.add(indexes[index - k - 1]);
      range.add(indexes[index - 1]);
    } else {
      List<Integer> range = new ArrayList<>();
      rangeList.add(range);
      range.add(indexes[index - 1]);

    }
    if (length != indexes.length) {
      List<Integer> range = new ArrayList<>();
      rangeList.add(range);
      range.add(indexes[indexes.length - 1]);
    }

    // as diving in range so array size will be always 2
    outputArray = new int[rangeList.size()][2];
    for (int i = 0; i < outputArray.length; i++) {
      if (rangeList.get(i).size() == 1) {
        outputArray[i][0] = rangeList.get(i).get(0);
        outputArray[i][1] = rangeList.get(i).get(0);
      } else {
        outputArray[i][0] = rangeList.get(i).get(0);
        outputArray[i][1] = rangeList.get(i).get(1);
      }
    }
    return outputArray;
  }

  public static void freeMemory(DimensionRawColumnChunk[] dimensionRawColumnChunks,
      MeasureRawColumnChunk[] measureRawColumnChunks) {
    if (null != measureRawColumnChunks) {
      for (int i = 0; i < measureRawColumnChunks.length; i++) {
        if (null != measureRawColumnChunks[i]) {
          measureRawColumnChunks[i].freeMemory();
        }
      }
    }
    if (null != dimensionRawColumnChunks) {
      for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
        if (null != dimensionRawColumnChunks[i]) {
          dimensionRawColumnChunks[i].freeMemory();
        }
      }
    }
  }

  /**
   * This method will check if dictionary and its metadata file exists for a given column
   *
   * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
   *                                         tableName and columnIdentifier
   * @return
   */
  public static boolean isFileExistsForGivenColumn(String carbonStorePath,
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    PathService pathService = CarbonCommonFactory.getPathService();
    CarbonTablePath carbonTablePath = pathService.getCarbonTablePath(carbonStorePath,
        dictionaryColumnUniqueIdentifier.getCarbonTableIdentifier());

    String dictionaryFilePath = carbonTablePath.getDictionaryFilePath(
        dictionaryColumnUniqueIdentifier.getColumnIdentifier().getColumnId());
    String dictionaryMetadataFilePath = carbonTablePath.getDictionaryMetaFilePath(
        dictionaryColumnUniqueIdentifier.getColumnIdentifier().getColumnId());
    // check if both dictionary and its metadata file exists for a given column
    return isFileExists(dictionaryFilePath) && isFileExists(dictionaryMetadataFilePath);
  }

  /**
   * @param tableInfo
   * @param invalidBlockVOForSegmentId
   * @param updateStatusMngr
   * @return
   */
  public static boolean isInvalidTableBlock(TableBlockInfo tableInfo,
      UpdateVO invalidBlockVOForSegmentId, SegmentUpdateStatusManager updateStatusMngr) {

    if (!updateStatusMngr.isBlockValid(tableInfo.getSegmentId(),
        CarbonTablePath.getCarbonDataFileName(tableInfo.getFilePath()) + CarbonTablePath
            .getCarbonDataExtension())) {
      return true;
    }

    UpdateVO updatedVODetails = invalidBlockVOForSegmentId;
    if (null != updatedVODetails) {
      Long blockTimeStamp = Long.parseLong(tableInfo.getFilePath()
          .substring(tableInfo.getFilePath().lastIndexOf('-') + 1,
              tableInfo.getFilePath().lastIndexOf('.')));
      if ((blockTimeStamp > updatedVODetails.getFactTimestamp() && (
          updatedVODetails.getUpdateDeltaStartTimestamp() != null
              && blockTimeStamp < updatedVODetails.getUpdateDeltaStartTimestamp()))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Below method will be used to get the format for
   * date or timestamp data type from property. This
   * is added to avoid the code duplication
   *
   * @param dataType
   * @return format
   */
  public static String getFormatFromProperty(DataType dataType) {
    switch (dataType) {
      case DATE:
        return CarbonProperties.getInstance().getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
            CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
      case TIMESTAMP:
        return CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
      default:
        return null;
    }
  }

  /**
   * Below method will be used to convert byte data to surrogate key based
   * column value size
   *
   * @param data                data
   * @param startOffsetOfData   start offset of data
   * @param eachColumnValueSize size of each column value
   * @return surrogate key
   */
  public static int getSurrogateInternal(byte[] data, int startOffsetOfData,
      int eachColumnValueSize) {
    int surrogate = 0;
    switch (eachColumnValueSize) {
      case 1:
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData] & 0xFF;
        return surrogate;
      case 2:
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 1] & 0xFF;
        return surrogate;
      case 3:
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 1] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 2] & 0xFF;
        return surrogate;
      case 4:
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 1] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 2] & 0xFF;
        surrogate <<= 8;
        surrogate ^= data[startOffsetOfData + 3] & 0xFF;
        return surrogate;
      default:
        throw new IllegalArgumentException("Int cannot me more than 4 bytes");
    }
  }
}

