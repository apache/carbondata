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
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.leafnode.DataFileFooter;
import org.carbondata.core.carbon.metadata.leafnode.datachunk.DataChunk;
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
import org.carbondata.core.metadata.CarbonMetadata.Dimension;
import org.carbondata.core.metadata.LeafNodeInfo;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.core.metadata.SliceMetaData;
import org.carbondata.core.metadata.ValueEncoderMeta;
import org.carbondata.core.reader.CarbonFooterReader;
import org.carbondata.core.vo.HybridStoreModel;
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
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                "Error while closing stream" + stream);
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

          LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e, "Error while getSortedFile");
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
   * This method copy the file from source to destination.
   *
   * @param sourceFile
   * @param fileDestination
   * @throws CarbonUtilException
   */
  public static void copySchemaFile(String sourceFile, String fileDestination)
      throws CarbonUtilException {
    FileInputStream fileInputStream = null;
    FileOutputStream fileOutputStream = null;

    File inputFile = new File(sourceFile);
    File outFile = new File(fileDestination);

    try {
      fileInputStream = new FileInputStream(inputFile);
      fileOutputStream = new FileOutputStream(outFile);

      byte[] buffer = new byte[1024];

      int length = 0;

      while ((length = fileInputStream.read(buffer)) > 0) {
        fileOutputStream.write(buffer, 0, length);
      }

    } catch (FileNotFoundException e) {
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e);
      throw new CarbonUtilException(
          "Proble while copying the file from: " + sourceFile + ": To" + fileDestination, e);
    } catch (IOException e) {
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e);
      throw new CarbonUtilException(
          "Proble while copying the file from: " + sourceFile + ": To" + fileDestination, e);
    } finally {
      closeStreams(fileInputStream, fileOutputStream);
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
   * @param dimCardinality                : dimension cardinality
   * @param dimensionStoreType            : dimension store type: true->columnar, false->row
   * @param NoDictionaryDimOrdinals
   * @param columnarStoreColumns->columns for columnar store
   * @param rowStoreColumns               -> columns for row store
   * @return
   */
  public static HybridStoreModel getHybridStoreMeta(int[] dimCardinality,
      boolean[] dimensionStoreType, List<Integer> noDictionaryDimOrdinals) {
    //get dimension store type
    HybridStoreModel hybridStoreMeta = new HybridStoreModel();

    List<Integer> columnarStoreOrdinalsList = new ArrayList<Integer>(dimensionStoreType.length);
    List<Integer> columnarDimcardinalityList = new ArrayList<Integer>(dimensionStoreType.length);
    List<Integer> rowStoreOrdinalsList = new ArrayList<Integer>(dimensionStoreType.length);
    List<Integer> rowDimCardinalityList = new ArrayList<Integer>(dimensionStoreType.length);
    boolean isHybridStore = false;
    for (int i = 0; i < dimensionStoreType.length; i++) {
      if (dimensionStoreType[i]) {
        columnarStoreOrdinalsList.add(i);
        columnarDimcardinalityList.add(dimCardinality[i]);

      } else {
        rowStoreOrdinalsList.add(i);
        rowDimCardinalityList.add(dimCardinality[i]);

      }
    }
    if (rowStoreOrdinalsList.size() > 0) {
      isHybridStore = true;
    }
    int[] columnarStoreOrdinal = convertToIntArray(columnarStoreOrdinalsList);
    int[] columnarDimcardinality = convertToIntArray(columnarDimcardinalityList);

    int[] rowStoreOrdinal = convertToIntArray(rowStoreOrdinalsList);
    int[] rowDimCardinality = convertToIntArray(rowDimCardinalityList);

    Map<Integer, Integer> dimOrdinalMDKeymapping = new HashMap<Integer, Integer>();
    Map<Integer, Integer> dimOrdinalStoreIndexMapping = new HashMap<Integer, Integer>();
    int dimCount = 0;
    int storeIndex = 0;
    for (int i = 0; i < rowStoreOrdinal.length; i++) {
      //dimOrdinalMDKeymapping.put(dimCount++, rowStoreOrdinal[i]);
      dimOrdinalMDKeymapping.put(rowStoreOrdinal[i], dimCount++);
      //row stores will be stored at 0th inex
      dimOrdinalStoreIndexMapping.put(rowStoreOrdinal[i], storeIndex);
    }
    if (rowStoreOrdinal.length > 0) {
      storeIndex++;
    }
    for (int i = 0; i < columnarStoreOrdinal.length; i++) {
      //dimOrdinalMDKeymapping.put(dimCount++,columnarStoreOrdinal[i] );
      dimOrdinalMDKeymapping.put(columnarStoreOrdinal[i], dimCount++);
      dimOrdinalStoreIndexMapping.put(columnarStoreOrdinal[i], storeIndex++);
    }

    //updating with highcardinality dimension store detail
    if (null != noDictionaryDimOrdinals) {
      for (Integer noDictionaryDimOrdinal : noDictionaryDimOrdinals) {
        dimOrdinalStoreIndexMapping.put(noDictionaryDimOrdinal, storeIndex++);
      }
    }

    //This split is used while splitting mdkey's into columns
    //1,1,1,3 -> it means first 3 dimension will be alone and next
    //3 dimension will be in single column

    //here in index +1 means total no of split will be all
    //dimension,part of columnar store, and one column which
    //will have all dimension in single column as row
    int[] mdKeyPartioner = null;
    int[][] dimensionPartitioner = null;
    // no of dimension stored as column.. this inculdes one complete set of row store
    int noOfColumnsStore = columnarStoreOrdinal.length;
    if (isHybridStore) {
      noOfColumnsStore++;
      mdKeyPartioner = new int[columnarStoreOrdinal.length + 1];
      dimensionPartitioner = new int[columnarDimcardinality.length + 1][];

      //row
      mdKeyPartioner[0] = rowDimCardinality.length;
      dimensionPartitioner[0] = new int[rowDimCardinality.length];
      for (int i = 0; i < rowDimCardinality.length; i++) {
        dimensionPartitioner[0][i] = rowDimCardinality[i];

      }
      //columnar
      //dimensionPartitioner[1]=new int[columnarDimcardinality.length];
      for (int i = 0; i < columnarDimcardinality.length; i++) {
        dimensionPartitioner[i + 1] = new int[] { columnarDimcardinality[i] };
        mdKeyPartioner[i + 1] = 1;
      }
    } else {
      mdKeyPartioner = new int[columnarStoreOrdinal.length];
      dimensionPartitioner = new int[columnarDimcardinality.length][];
      //columnar
      dimensionPartitioner[0] = new int[columnarDimcardinality.length];
      for (int i = 0; i < columnarDimcardinality.length; i++) {
        dimensionPartitioner[i] = new int[] { columnarDimcardinality[i] };
        mdKeyPartioner[i] = 1;
      }
    }

    hybridStoreMeta.setNoOfColumnStore(noOfColumnsStore);
    hybridStoreMeta.setDimOrdinalMDKeyMapping(dimOrdinalMDKeymapping);
    hybridStoreMeta.setColumnStoreOrdinals(columnarStoreOrdinal);
    hybridStoreMeta.setRowStoreOrdinals(rowStoreOrdinal);
    hybridStoreMeta.setDimensionPartitioner(dimensionPartitioner);
    hybridStoreMeta.setColumnSplit(mdKeyPartioner);
    hybridStoreMeta.setDimOrdinalStoreIndexMapping(dimOrdinalStoreIndexMapping);
    //this is no

    //get Key generator for each columnar and row store
    int[] completeCardinality = new int[rowDimCardinality.length + columnarDimcardinality.length];
    System.arraycopy(rowDimCardinality, 0, completeCardinality, 0, rowDimCardinality.length);
    System.arraycopy(columnarDimcardinality, 0, completeCardinality, rowDimCardinality.length,
        columnarDimcardinality.length);

    hybridStoreMeta.setHybridCardinality(completeCardinality);

    hybridStoreMeta.setHybridStore(isHybridStore);

    return hybridStoreMeta;
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
   * This method will return bit length required for each dimension based on splits
   *
   * @param dimension
   * @param dimPartitioner : this will partition few dimension to be stored
   *                       at row level. If it is row level than data is store in bits
   * @return
   */
  public static int[] getDimensionBitLength(int[] dimCardinality, int[][] dimPartitioner) {
    int[] newdims = new int[dimCardinality.length];
    int dimCounter = 0;
    for (int i = 0; i < dimPartitioner.length; i++) {
      if (dimCardinality[i] == 0) {
        //Array or struct type may have higher value
        newdims[i] = 64;
      } else if (dimPartitioner[i].length == 1) {
        //for columnar store
        newdims[dimCounter] = getBitLengthFullyFilled(dimCardinality[dimCounter]);
        dimCounter++;
      } else {
        // for row store
        int totalSize = 0;
        for (int j = 0; j < dimPartitioner[i].length; j++) {
          newdims[dimCounter] = getIncrementedCardinality(dimCardinality[dimCounter]);
          totalSize += newdims[dimCounter];
          dimCounter++;
        }
        //need to check if its required
        int mod = totalSize % 8;
        if (mod > 0) {
          newdims[dimCounter - 1] = newdims[dimCounter - 1] + (8 - mod);
        }
      }
    }
    return newdims;
  }

  /**
   * This method will be used to update the dimension cardinality
   *
   * @param dimCardinality
   * @return new increment cardinality
   */
  public static int getIncrementedFullyFilledRCDCardinalityFullyFilled(int dimCardinality) {
    int bitsLength = Long.toBinaryString(dimCardinality).length();
    int div = bitsLength / 8;
    int mod = bitsLength % 8;
    if (mod > 0) {
      dimCardinality = 8 * (div + 1);
    } else {
      dimCardinality = bitsLength;
    }
    return dimCardinality;
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
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                "Problem renaming the cube :: " + fullPath);
            c = new DeleteCube(file);
            executorService.submit(c);
          } else {
            c = new DeleteCube(FileFactory.getCarbonFile(newFilePath, fileType));
            executorService.submit(c);
          }
        }
      } catch (IOException e) {
        LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
            "Problem renaming the cube :: " + fullPath);
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
   * @return will return leaf node info which will have all the meta data
   * related to leaf file
   */
  public static List<LeafNodeInfo> getLeafNodeInfo(File file, int measureCount, int mdKeySize) {
    List<LeafNodeInfo> listOfNodeInfo =
        new ArrayList<LeafNodeInfo>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    String filesLocation = file.getAbsolutePath();
    long fileSize = file.length();
    return getLeafNodeDetails(listOfNodeInfo, filesLocation, measureCount, mdKeySize, fileSize);
  }

  /**
   * This method will be used to read leaf meta data format of meta data will be
   * <entrycount><keylength><keyoffset><measure1length><measure1offset>
   *
   * @param file
   * @param measureCount
   * @param mdKeySize
   * @return will return leaf node info which will have all the meta data
   * related to leaf file
   */
  public static List<LeafNodeInfo> getLeafNodeInfo(CarbonFile file, int measureCount,
      int mdKeySize) {
    List<LeafNodeInfo> listOfNodeInfo =
        new ArrayList<LeafNodeInfo>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    String filesLocation = file.getAbsolutePath();
    long fileSize = file.getSize();
    return getLeafNodeDetails(listOfNodeInfo, filesLocation, measureCount, mdKeySize, fileSize);
  }

  /**
   * @param listOfNodeInfo
   * @param filesLocation
   * @param measureCount
   * @param mdKeySize
   * @param fileSize
   * @return
   */
  private static List<LeafNodeInfo> getLeafNodeDetails(List<LeafNodeInfo> listOfNodeInfo,
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
      LeafNodeInfo info = new LeafNodeInfo();
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
   * This method will be used to read leaf meta data format of meta data will
   * be <entrycount><keylength><keyoffset><measure1length><measure1offset>
   *
   * @param file
   * @param measureCount
   * @param mdKeySize
   * @return will return leaf node info which will have all the meta data
   * related to leaf file
   */
  public static List<LeafNodeInfoColumnar> getLeafNodeInfoColumnar(File file, int measureCount,
      int mdKeySize) {
    List<LeafNodeInfoColumnar> listOfNodeInfo =
        new ArrayList<LeafNodeInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    String filesLocation = file.getAbsolutePath();
    long fileSize = file.length();
    return getLeafNodeInfo(measureCount, mdKeySize, listOfNodeInfo, filesLocation, fileSize);
  }

  /**
   * This method will be used to read leaf meta data format of meta data will
   * be <entrycount><keylength><keyoffset><measure1length><measure1offset>
   *
   * @param file
   * @param measureCount
   * @param mdKeySize
   * @return will return leaf node info which will have all the meta data
   * related to leaf file
   */
  public static List<LeafNodeInfoColumnar> getLeafNodeInfoColumnar(CarbonFile file,
      int measureCount, int mdKeySize) {
    List<LeafNodeInfoColumnar> listOfNodeInfo =
        new ArrayList<LeafNodeInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    String filesLocation = file.getAbsolutePath();
    long fileSize = file.getSize();
    return getLeafNodeInfo(measureCount, mdKeySize, listOfNodeInfo, filesLocation, fileSize);
  }

  /**
   * @param measureCount
   * @param mdKeySize
   * @param listOfNodeInfo
   * @param filesLocation
   * @param fileSize
   * @return
   */
  private static List<LeafNodeInfoColumnar> getLeafNodeInfo(int measureCount, int mdKeySize,
      List<LeafNodeInfoColumnar> listOfNodeInfo, String filesLocation, long fileSize) {
    long offset = fileSize - CarbonCommonConstants.LONG_SIZE_IN_BYTE;
    FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(filesLocation));
    offset = fileHolder.readDouble(filesLocation, offset);
    CarbonFooterReader metaDataReader = new CarbonFooterReader(filesLocation, offset);
    try {
      listOfNodeInfo = CarbonMetadataUtil.convertLeafNodeInfo(metaDataReader.readFooter());
    } catch (IOException e) {
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
          "Problem while reading metadata :: " + filesLocation, e);
    }
    for (LeafNodeInfoColumnar infoColumnar : listOfNodeInfo) {
      infoColumnar.setFileName(filesLocation);
    }
    return listOfNodeInfo;
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
      LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
          "Slice Metadata file Path: " + path + '/' + CarbonUtil
              .getSliceMetaDataFileName(nextRestructFolder));
      stream = FileFactory
          .getDataOutputStream(path + File.separator + getSliceMetaDataFileName(nextRestructFolder),
              FileFactory.getFileType(path));
      objectOutputStream = new ObjectOutputStream(stream);
      objectOutputStream.writeObject(sliceMetaData);
    } catch (IOException e) {
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e.getMessage());
    } finally {
      closeStreams(objectOutputStream, stream);
    }
  }

  public static File[] listFile(String location, final String fileNameInitial,
      final String fileNameExt) {
    File file = new File(location);
    File[] listFiles = file.listFiles(new FileFilter() {
      @Override public boolean accept(File pathname) {
        String name = pathname.getName();

        return name.startsWith(fileNameInitial) && name.endsWith(fileNameExt);
      }
    });
    return listFiles;
  }

  public static void deleteFiles(File[] intermediateFiles) throws CarbonUtilException {
    for (int i = 0; i < intermediateFiles.length; i++) {
      if (!intermediateFiles[i].delete()) {
        throw new CarbonUtilException("Problem while deleting intermediate file");
      }
    }
  }

  /**
   * Below method will be used to get the fact file present in slice
   *
   * @param sliceLocation slice location
   * @return fact files array
   */
  public static File[] getAllFactFiles(String sliceLocation, final String tableName) {
    File file = new File(sliceLocation);
    File[] files = null;
    if (file.isDirectory()) {
      files = file.listFiles(new FileFilter() {
        public boolean accept(File pathname) {
          return ((!pathname.isDirectory()) && (pathname.getName().startsWith(tableName))
              && pathname.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT));

        }
      });
    }
    return files;
  }

  /**
   * This method will be used to for sending the new slice signal to engine
   */
  public static void flushSEQGenLruCache(String key) throws CarbonUtilException {
    try {
      // inform engine to load new slice
      Class<?> c = Class.forName("com.huawei.unibi.carbon.surrogatekeysgenerator.lru.LRUCache");
      Class[] argTypes = new Class[] {};
      // get the instance of CubeSliceLoader
      Method main = c.getDeclaredMethod("getInstance", argTypes);
      Object invoke = main.invoke(null, null);
      Class[] argTypes1 = new Class[] { String.class };

      // ionvoke loadSliceFromFile
      Method declaredMethod = c.getDeclaredMethod("remove", argTypes1);
      // pass cube name and store location
      String[] a = { key };
      declaredMethod.invoke(invoke, a);
    } catch (ClassNotFoundException classNotFoundException) {
      throw new CarbonUtilException("Problem while flushin the seqgen lru cache",
          classNotFoundException);
    } catch (NoSuchMethodException noSuchMethodException) {
      throw new CarbonUtilException("Problem while flushin the seqgen lru cache",
          noSuchMethodException);
    } catch (IllegalAccessException illegalAccessException) {
      throw new CarbonUtilException("Problem while flushin the seqgen lru cache",
          illegalAccessException);
    } catch (InvocationTargetException invocationTargetException) {
      throw new CarbonUtilException("Problem while flushin the seqgen lru cache",
          invocationTargetException);
    }
  }

  public static short[] getUnCompressColumnIndex(int totalLength, byte[] columnIndexData) {
    ByteBuffer buffer = ByteBuffer.wrap(columnIndexData);
    buffer.rewind();
    int indexDataLength = buffer.getInt();
    short[] indexData = new short[indexDataLength / 2];
    short[] indexMap =
        new short[(totalLength - indexDataLength - CarbonCommonConstants.INT_SIZE_IN_BYTE) / 2];
    int counter = 0;
    while (counter < indexData.length) {
      indexData[counter] = buffer.getShort();
      counter++;
    }
    counter = 0;
    while (buffer.hasRemaining()) {
      indexMap[counter++] = buffer.getShort();
    }
    return UnBlockIndexer.uncompressIndex(indexData, indexMap);
  }

  public static ColumnarKeyStoreInfo getColumnarKeyStoreInfo(LeafNodeInfoColumnar leafNodeInfo,
      int[] eachBlockSize, HybridStoreModel hybridStoreMeta) {
    ColumnarKeyStoreInfo columnarKeyStoreInfo = new ColumnarKeyStoreInfo();
    columnarKeyStoreInfo.setFilePath(leafNodeInfo.getFileName());
    columnarKeyStoreInfo.setIsSorted(leafNodeInfo.getIsSortedKeyColumn());
    columnarKeyStoreInfo.setKeyBlockIndexLength(leafNodeInfo.getKeyBlockIndexLength());
    columnarKeyStoreInfo.setKeyBlockIndexOffsets(leafNodeInfo.getKeyBlockIndexOffSets());
    columnarKeyStoreInfo.setKeyBlockLengths(leafNodeInfo.getKeyLengths());
    columnarKeyStoreInfo.setKeyBlockOffsets(leafNodeInfo.getKeyOffSets());
    columnarKeyStoreInfo.setNumberOfKeys(leafNodeInfo.getNumberOfKeys());
    columnarKeyStoreInfo.setSizeOfEachBlock(eachBlockSize);
    columnarKeyStoreInfo.setNumberCompressor(new NumberCompressor(Integer.parseInt(
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.LEAFNODE_SIZE,
            CarbonCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL))));
    columnarKeyStoreInfo.setAggKeyBlock(leafNodeInfo.getAggKeyBlock());
    columnarKeyStoreInfo.setDataIndexMapLength(leafNodeInfo.getDataIndexMapLength());
    columnarKeyStoreInfo.setDataIndexMapOffsets(leafNodeInfo.getDataIndexMapOffsets());
    columnarKeyStoreInfo.setHybridStoreModel(hybridStoreMeta);
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

  public static int getLastIndexUsingBinarySearch(ColumnarKeyStoreDataHolder keyBlockArray, int low,
      int high, byte[] compareValue, int numberOfRows) {
    int cmpResult = 0;
    while (high >= low) {
      int mid = (low + high) / 2;
      cmpResult = ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(keyBlockArray.getKeyBlockData(), mid * compareValue.length,
              compareValue.length, compareValue, 0, compareValue.length);
      if (cmpResult == 0 && (mid == numberOfRows - 1 || ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(keyBlockArray.getKeyBlockData(), (mid + 1) * compareValue.length,
              compareValue.length, compareValue, 0, compareValue.length) > 0)) {
        return mid;
      } else if (cmpResult > 0) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }

    return -1;
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

  public static int getFirstIndexUsingBinarySearch(ColumnarKeyStoreDataHolder keyBlockArray,
      int low, int high, byte[] compareValue, int numberOfRows, int length, int offset) {
    int cmpResult = 0;
    while (high >= low) {
      int mid = (low + high) / 2;
      cmpResult = ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(keyBlockArray.getKeyBlockData(), mid * compareValue.length,
              compareValue.length, compareValue, 0, compareValue.length);
      if (cmpResult == 0 && (mid == 0 || ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(keyBlockArray.getKeyBlockData(), (mid - 1) * length, length, compareValue,
              offset, length) < 0)) {
        return mid;
      } else if (cmpResult < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return -1;
  }

  public static int getIndexUsingBinarySearch(ColumnarKeyStoreDataHolder keyBlockArray, int low,
      int high, byte[] compareValue) {
    int cmpResult = 0;
    while (high >= low) {
      int mid = (low + high) / 2;
      cmpResult = ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(keyBlockArray.getKeyBlockData(), mid * compareValue.length,
              compareValue.length, compareValue, 0, compareValue.length);
      if (cmpResult == 0) {
        return mid;
      } else if (cmpResult < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return -1;
  }

  public static int getIndexUsingBinarySearch(ColumnarKeyStoreDataHolder keyBlockArray, int low,
      int high, byte[] compareValue, int lenghtToCompare, int offset) {
    int cmpResult = 0;
    while (high >= low) {
      int mid = (low + high) / 2;
      cmpResult = ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(keyBlockArray.getKeyBlockData(), mid * lenghtToCompare, lenghtToCompare,
              compareValue, offset, lenghtToCompare);
      if (cmpResult == 0) {
        return mid;
      } else if (cmpResult < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return -1;
  }

  public static int byteArrayBinarySearch(byte[][] filter, ColumnarKeyStoreDataHolder keyBlockArray,
      int index) {
    int low = 0;
    int high = filter.length - 1;
    int mid = 0;
    int cmp = 0;
    while (high >= low) {
      mid = (low + high) >>> 1;
      cmp = ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(keyBlockArray.getKeyBlockData(), index * filter[mid].length,
              filter[mid].length, filter[mid], 0, filter[mid].length);
      if (cmp == 0) {
        return 0;
      } else if (cmp < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
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
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, "Error while compressColumn Index ", e,
          e.getMessage());
    }
    return UnBlockIndexer.uncompressIndex(numberCompressor.unCompress(indexData),
        numberCompressor.unCompress(indexMap));
  }

  public static boolean[] convertToBooleanArray(List<Boolean> needCompressedDataList) {
    boolean[] needCompressedData = new boolean[needCompressedDataList.size()];
    for (int i = 0; i < needCompressedData.length; i++) {
      needCompressedData[i] = needCompressedDataList.get(i);
    }
    return needCompressedData;
  }

  public static int[] convertToIntArray(List<Integer> list) {
    int[] array = new int[list.size()];
    for (int i = 0; i < array.length; i++) {
      array[i] = list.get(i);
    }
    return array;
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

  public static String[] getSlices(String storeLocation, String tableName,
      FileFactory.FileType fileType) {
    try {
      if (!FileFactory.isFileExist(storeLocation, fileType)) {
        return new String[0];
      }
    } catch (IOException e) {
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, "Error occurred :: " + e.getMessage());
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
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
          "Error while writing RS meta file : " + fullFileName + e.getMessage());
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

      LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
          "Level cardinality file written to : " + levelCardinalityFilePath);
    } catch (IOException e) {
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
          "Error while writing level cardinality file : " + levelCardinalityFilePath + e
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
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e);
    } catch (FileNotFoundException e) {
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
          "@@@@@ SliceMetaData File is missing @@@@@ :" + path);
    } catch (IOException e) {
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
          "@@@@@ Error while reading SliceMetaData File @@@@@ :" + path);
    } finally {
      closeStreams(objectInputStream, stream);
    }
    return readObject;
  }

  public static SliceMetaData readSliceMetaDataFile(String folderPath, int currentRestructNumber) {
    String path = folderPath + '/' + getSliceMetaDataFileName(currentRestructNumber);
    return readSliceMetaDataFile(path);
  }

  public static SliceMetaData readSliceMetaDataFile(CarbonFile folderPath) {
    CarbonFile[] sliceMetaDataPath = folderPath.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile file) {
        return file.getName().startsWith("sliceMetaData");
      }
    });

    if (null == sliceMetaDataPath || sliceMetaDataPath.length < 1) {
      return null;
    }
    Arrays.sort(sliceMetaDataPath, new SliceMetaDataFileComparator());
    return readSliceMetaDataFile(sliceMetaDataPath[sliceMetaDataPath.length - 1].getAbsolutePath());
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
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
          "@@@@@@  File not found at a given location @@@@@@ : " + fileName);
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
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e.getMessage());
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
   * This API will record the indexes of the dimension which doesnt have
   * Dictionary values.
   *
   * @param currentDims .
   * @return
   */
  public static int[] getNoDictionaryColIndex(Dimension[] currentDims) {
    List<Integer> dirSurrogateList = new ArrayList<Integer>(currentDims.length);
    for (Dimension dim : currentDims) {
      if (dim.isNoDictionaryDim()) {
        dirSurrogateList.add(dim.getOrdinal());
      }
    }
    int[] noDictionaryValIndex =
        ArrayUtils.toPrimitive(dirSurrogateList.toArray(new Integer[dirSurrogateList.size()]));
    return noDictionaryValIndex;
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
   * @param dimension
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
   * @param offset   offset in the file
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

  private static class SliceMetaDataFileComparator implements Comparator<CarbonFile> {

    @Override public int compare(CarbonFile o1, CarbonFile o2) {
      int firstSliceNumber = Integer.parseInt(o1.getName().split("\\.")[1]);
      int secondSliceNumber = Integer.parseInt(o2.getName().split("\\.")[1]);
      return firstSliceNumber - secondSliceNumber;
    }

  }

}

