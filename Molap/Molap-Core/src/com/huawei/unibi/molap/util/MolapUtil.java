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

package com.huawei.unibi.molap.util;

import java.io.*;
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

import com.google.gson.Gson;
import com.huawei.datasight.molap.core.load.LoadMetadataDetails;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreInfo;
import com.huawei.unibi.molap.datastorage.store.columnar.UnBlockIndexer;
import com.huawei.unibi.molap.datastorage.store.fileperations.AtomicFileOperations;
import com.huawei.unibi.molap.datastorage.store.fileperations.AtomicFileOperationsImpl;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFileFilter;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.keygenerator.mdkey.NumberCompressor;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;
import com.huawei.unibi.molap.metadata.SliceMetaData;
import com.huawei.unibi.molap.vo.HybridStoreModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.pentaho.di.core.exception.KettleException;

public final class MolapUtil {

    private static final String HDFS_PREFIX = "hdfs://";

    private static final String FS_DEFAULT_FS = "fs.defaultFS";

    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MolapUtil.class.getName());

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

    private MolapUtil() {

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
                        LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
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

                    LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e,
                            "Error while getSortedFile");
                    return 0;
                }
            }
        });
        return fileArray;
    }

    public static MolapFile[] getSortedFileList(MolapFile[] fileArray) {
        Arrays.sort(fileArray, new Comparator<MolapFile>() {
            public int compare(MolapFile o1, MolapFile o2) {
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

        MolapFile molapFile =
                FileFactory.getMolapFile(baseStorePath, FileFactory.getFileType(baseStorePath));

        // List of directories
        MolapFile[] listFiles = molapFile.listFiles(new MolapFileFilter() {
            @Override public boolean accept(MolapFile pathname) {
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
    private static int findCounterValue(final String filterType, MolapFile[] listFiles,
            int counter) {
        if ("Load_".equals(filterType)) {
            for (MolapFile files : listFiles) {
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
            for (MolapFile eachFile : listFiles) {
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
    private static String getFolderName(MolapFile eachFile) {
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
     * @throws MolapUtilException
     */
    public static void copySchemaFile(String sourceFile, String fileDestination)
            throws MolapUtilException {
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
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e);
            throw new MolapUtilException(
                    "Proble while copying the file from: " + sourceFile + ": To" + fileDestination,
                    e);
        } catch (IOException e) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e);
            throw new MolapUtilException(
                    "Proble while copying the file from: " + sourceFile + ": To" + fileDestination,
                    e);
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
        final int incrValue = Integer.parseInt(MolapProperties.getInstance()
                .getProperty(MolapCommonConstants.CARDINALITY_INCREMENT_VALUE,
                        MolapCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL));

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
    
    public static int getIncrementedCardinality(int dimCardinality)
    {
        // get the cardinality incr factor
        final int incrValue = Integer.parseInt(MolapProperties.getInstance().getProperty(
                MolapCommonConstants.CARDINALITY_INCREMENT_VALUE,
                MolapCommonConstants.CARDINALITY_INCREMENT_VALUE_DEFAULT_VAL));

        int perIncr = 0;
        int remainder = 0;
        int newDimsC = 0;

        // get the incr
        perIncr = (dimCardinality * incrValue) / CONST_HUNDRED;

        // if per incr is more than one the add to cardinality
        if(perIncr > 0)
        {
            newDimsC = dimCardinality + perIncr;
        }
        else
        {
            // else add one
            newDimsC = dimCardinality + 1;
        }
        // check whether its in boundary condition
        remainder = newDimsC % CONST_EIGHT;
        if(remainder == CONST_SEVEN)
        {
            // then incr cardinality by 1
            newDimsC = dimCardinality + 1;
        }
        newDimsC = Long.toBinaryString(newDimsC).length();
        // get the log bits of cardinality

        return newDimsC;
    }

      /**
     *  
     * @param dimCardinality : dimension cardinality
     * @param dimensionStoreType : dimension store type: true->columnar, false->row
     * @param highCardDimOrdinals 
     * @param columnarStoreColumns->columns for columnar store
     * @param rowStoreColumns -> columns for row store
     * @return
     */
    public static HybridStoreModel getHybridStoreMeta(int[] dimCardinality,boolean[] dimensionStoreType, List<Integer> highCardDimOrdinals)
    {
        //get dimension store type
        HybridStoreModel hybridStoreMeta=new HybridStoreModel();
      
        List<Integer> columnarStoreOrdinalsList = new ArrayList<Integer>(dimensionStoreType.length);
        List<Integer> columnarDimcardinalityList =new ArrayList<Integer>(dimensionStoreType.length);
        List<Integer> rowStoreOrdinalsList = new ArrayList<Integer>(dimensionStoreType.length);
        List<Integer> rowDimCardinalityList= new ArrayList<Integer>(dimensionStoreType.length);
        boolean isHybridStore=false;
        for (int i = 0; i < dimensionStoreType.length; i++)
        {
            if (dimensionStoreType[i])
            {
                columnarStoreOrdinalsList.add(i);
                columnarDimcardinalityList.add(dimCardinality[i]);
                
            }
            else
            {
                rowStoreOrdinalsList.add(i);
                rowDimCardinalityList.add(dimCardinality[i]);
               
            }
        }
        if(rowStoreOrdinalsList.size()>0)
        {
            isHybridStore=true;
        }
        int[] columnarStoreOrdinal=convertToIntArray(columnarStoreOrdinalsList);
        int[] columnarDimcardinality = convertToIntArray(columnarDimcardinalityList);
        
        int[] rowStoreOrdinal=convertToIntArray(rowStoreOrdinalsList);
        int[] rowDimCardinality=convertToIntArray(rowDimCardinalityList);
        
        Map<Integer,Integer> dimOrdinalMDKeymapping=new HashMap<Integer,Integer>();
        Map<Integer,Integer> dimOrdinalStoreIndexMapping=new HashMap<Integer,Integer>();
        int dimCount=0;
        int storeIndex=0;
        for(int i=0;i<rowStoreOrdinal.length;i++)
        {
            //dimOrdinalMDKeymapping.put(dimCount++, rowStoreOrdinal[i]);
            dimOrdinalMDKeymapping.put(rowStoreOrdinal[i],dimCount++);
            //row stores will be stored at 0th inex
            dimOrdinalStoreIndexMapping.put(rowStoreOrdinal[i], storeIndex);
        }
        if(rowStoreOrdinal.length>0)
        {
        	storeIndex++;
        }
        for(int i=0;i<columnarStoreOrdinal.length;i++)
        {
            //dimOrdinalMDKeymapping.put(dimCount++,columnarStoreOrdinal[i] );
            dimOrdinalMDKeymapping.put(columnarStoreOrdinal[i],dimCount++);
            dimOrdinalStoreIndexMapping.put(columnarStoreOrdinal[i], storeIndex++);
        }
       
        //updating with highcardinality dimension store detail
        if(null!=highCardDimOrdinals)
        {
            for(Integer highCardDimOrdinal:highCardDimOrdinals)
            {
                dimOrdinalStoreIndexMapping.put(highCardDimOrdinal, storeIndex++);
            }    
        }
        
        
        //This split is used while splitting mdkey's into columns
        //1,1,1,3 -> it means first 3 dimension will be alone and next 3 dimension will be in single column
        
        //here in index +1 means total no of split will be all dimension,part of columnar store, and one column which
        //will have all dimension in single column as row
        int[] mdKeyPartioner=null;
        int[][] dimensionPartitioner=null;
        // no of dimension stored as column.. this inculdes one complete set of row store
        int noOfColumnsStore=columnarStoreOrdinal.length;
        if(isHybridStore)
        {
            noOfColumnsStore++;
            mdKeyPartioner=new int[columnarStoreOrdinal.length+1];
            dimensionPartitioner=new int[columnarDimcardinality.length+1][];
            
            //row
            mdKeyPartioner[0]=rowDimCardinality.length;
            dimensionPartitioner[0]=new int[rowDimCardinality.length];
            for(int i=0;i<rowDimCardinality.length;i++)
            {
                dimensionPartitioner[0][i]=rowDimCardinality[i];
               
            }
            //columnar
            //dimensionPartitioner[1]=new int[columnarDimcardinality.length];
            for(int i=0;i<columnarDimcardinality.length;i++)
            {
                dimensionPartitioner[i+1]=new int[]{columnarDimcardinality[i]};
                mdKeyPartioner[i+1]=1;
            }
        }
        else
        {
            mdKeyPartioner=new int[columnarStoreOrdinal.length];
            dimensionPartitioner=new int[columnarDimcardinality.length][];
            //columnar
            dimensionPartitioner[0]=new int[columnarDimcardinality.length];
            for(int i=0;i<columnarDimcardinality.length;i++)
            {
                dimensionPartitioner[i]=new int[]{columnarDimcardinality[i]};
                mdKeyPartioner[i]=1;
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
        int[] completeCardinality=new int[rowDimCardinality.length+columnarDimcardinality.length];
        System.arraycopy(rowDimCardinality, 0, completeCardinality, 0, rowDimCardinality.length);
        System.arraycopy(columnarDimcardinality, 0, completeCardinality,rowDimCardinality.length, columnarDimcardinality.length);
        
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

    private static int getBitLengthFullyFilled(int dimlens)
    {
        int bitsLength=Long.toBinaryString(dimlens).length();
        int div=bitsLength/8;
        int mod=bitsLength%8;
        if(mod>0)
        {
            return 8 * (div + 1);
        }
        else
        {
            return bitsLength;
        }
    }
     /**
     * This method will return bit length required for each dimension based on splits
     * @param dimension
     * @param dimPartitioner : this will partition few dimension to be stored at row level. If it is row level than data is store in bits 
     * @return
     */
    public static int[] getDimensionBitLength(int[] dimCardinality, int[][] dimPartitioner)
    {
        int[] newdims=new int[dimCardinality.length];
        int dimCounter=0;
        for(int i=0;i<dimPartitioner.length;i++)
        {
            if(dimCardinality[i] == 0)
            {
                //Array or struct type may have higher value 
                newdims[i]=64;
            }
            else if(dimPartitioner[i].length==1)
            {
                //for columnar store
                newdims[dimCounter]=getBitLengthFullyFilled(dimCardinality[dimCounter]);
                dimCounter++;
            }
            else
            {
                // for row store
                int totalSize=0;
                for(int j=0;j<dimPartitioner[i].length;j++)
                {
                    newdims[dimCounter]=MolapUtil.getIncrementedCardinality(dimCardinality[dimCounter]);
                    totalSize+=newdims[dimCounter];
                    dimCounter++;
                }
                //need to check if its required
                int mod=totalSize%8;
                if(mod>0)
                {
                    newdims[dimCounter-1]=newdims[dimCounter-1]+(8-mod);
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
    public static void deleteFoldersAndFiles(final String... path) throws MolapUtilException {
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
            throw new MolapUtilException("Error while deleteing the folders and files");
        } catch (InterruptedException e) {
            throw new MolapUtilException("Error while deleteing the folders and files");
        }
    }

    /**
     * This method will be used to delete the folder and files
     *
     * @param path file path array
     * @throws Exception exception
     */
    public static void deleteFoldersAndFiles(final File... path) throws MolapUtilException {
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
            throw new MolapUtilException("Error while deleteing the folders and files");
        } catch (InterruptedException e) {
            throw new MolapUtilException("Error while deleteing the folders and files");
        }

    }

    /**
     * Recursively delete the files
     *
     * @param f File to be deleted
     * @throws MolapUtilException
     */
    private static void deleteRecursive(File f) throws MolapUtilException {
        if (f.isDirectory()) {
            if (f.listFiles() != null) {
                for (File c : f.listFiles()) {
                    deleteRecursive(c);
                }
            }
        }
        if (f.exists() && !f.delete()) {
            throw new MolapUtilException("Error while deleteing the folders and files");
        }
    }

    public static void deleteFoldersAndFiles(final MolapFile... file) throws MolapUtilException {
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
            throw new MolapUtilException("Error while deleteing the folders and files");
        } catch (InterruptedException e) {
            throw new MolapUtilException("Error while deleteing the folders and files");
        }
    }

    public static void deleteFoldersAndFilesSilent(final MolapFile... file)
            throws MolapUtilException {
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
            throw new MolapUtilException("Error while deleteing the folders and files");
        } catch (InterruptedException e) {
            throw new MolapUtilException("Error while deleteing the folders and files");
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
    public static void renameCubeForDeletion(int partitionCount, String storePath,
            String schemaName, String cubeName) {
        String cubeNameWithPartition = "";
        String schemaNameWithPartition = "";
        String fullPath = "";
        String newFilePath = "";
        String newFileName = "";
        Callable<Void> c = null;
        long time = System.currentTimeMillis();
        FileType fileType = null;
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < partitionCount; i++) {
            schemaNameWithPartition = schemaName + '_' + i;
            cubeNameWithPartition = cubeName + '_' + i;
            newFileName = cubeNameWithPartition + '_' + time;
            fullPath = storePath + File.separator + schemaNameWithPartition + File.separator
                    + cubeNameWithPartition;
            newFilePath = storePath + File.separator + schemaNameWithPartition + File.separator
                    + newFileName;
            fileType = FileFactory.getFileType(fullPath);
            try {
                if (FileFactory.isFileExist(fullPath, fileType)) {
                    MolapFile file = FileFactory.getMolapFile(fullPath, fileType);
                    boolean isRenameSuccessfull = file.renameTo(newFilePath);
                    if (!isRenameSuccessfull) {
                        LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                                "Problem renaming the cube :: " + fullPath);
                        c = new DeleteCube(file);
                        executorService.submit(c);
                    } else {
                        c = new DeleteCube(FileFactory.getMolapFile(newFilePath, fileType));
                        executorService.submit(c);
                    }
                }
            } catch (IOException e) {
                LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                        "Problem renaming the cube :: " + fullPath);
            }
        }
        executorService.shutdown();
    }

    /**
     * Recursively delete the files
     *
     * @param f File to be deleted
     * @throws MolapUtilException
     */
    private static void deleteRecursive(MolapFile f) throws MolapUtilException {
        if (f.isDirectory()) {
            if (f.listFiles() != null) {
                for (MolapFile c : f.listFiles()) {
                    deleteRecursive(c);
                }
            }
        }
        if (f.exists() && !f.delete()) {
            throw new MolapUtilException("Error while deleteing the folders and files");
        }
    }

    private static void deleteRecursiveSilent(MolapFile f) throws MolapUtilException {
        if (f.isDirectory()) {
            if (f.listFiles() != null) {
                for (MolapFile c : f.listFiles()) {
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
                new ArrayList<LeafNodeInfo>(MolapCommonConstants.CONSTANT_SIZE_TEN);
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
    public static List<LeafNodeInfo> getLeafNodeInfo(MolapFile file, int measureCount,
            int mdKeySize) {
        List<LeafNodeInfo> listOfNodeInfo =
                new ArrayList<LeafNodeInfo>(MolapCommonConstants.CONSTANT_SIZE_TEN);
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
        long offset = fileSize - MolapCommonConstants.LONG_SIZE_IN_BYTE;
        FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(filesLocation));
        offset = fileHolder.readDouble(filesLocation, offset);
        int totalMetaDataLength =
                (int) (fileSize - MolapCommonConstants.LONG_SIZE_IN_BYTE - offset);
        ByteBuffer buffer = ByteBuffer
                .wrap(fileHolder.readByteArray(filesLocation, offset, totalMetaDataLength));
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
                new ArrayList<LeafNodeInfoColumnar>(MolapCommonConstants.CONSTANT_SIZE_TEN);
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
    public static List<LeafNodeInfoColumnar> getLeafNodeInfoColumnar(MolapFile file,
            int measureCount, int mdKeySize) {
        List<LeafNodeInfoColumnar> listOfNodeInfo =
                new ArrayList<LeafNodeInfoColumnar>(MolapCommonConstants.CONSTANT_SIZE_TEN);
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
        long offset = fileSize - MolapCommonConstants.LONG_SIZE_IN_BYTE;
        FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(filesLocation));
        offset = fileHolder.readDouble(filesLocation, offset);
        int totalMetaDataLength =
                (int) (fileSize - MolapCommonConstants.LONG_SIZE_IN_BYTE - offset);
        ByteBuffer buffer = ByteBuffer
                .wrap(fileHolder.readByteArray(filesLocation, offset, totalMetaDataLength));
        buffer.rewind();
        while (buffer.hasRemaining()) {
            //
            int[] msrLength = new int[measureCount];
            long[] msrOffset = new long[measureCount];
            LeafNodeInfoColumnar info = new LeafNodeInfoColumnar();
            byte[] startKey = new byte[mdKeySize];
            byte[] endKey = new byte[mdKeySize];
            info.setFileName(filesLocation);
            info.setNumberOfKeys(buffer.getInt());
            int keySplitValue = buffer.getInt();
            setInfo(buffer, info, startKey, endKey, keySplitValue);
            info.setEndKey(endKey);
            for (int i = 0; i < measureCount; i++) {
                msrLength[i] = buffer.getInt();
                msrOffset[i] = buffer.getLong();
            }
            int numberOfKeyBlockInfo = buffer.getInt();
            int[] keyIndexBlockLengths = new int[numberOfKeyBlockInfo];
            long[] keyIndexBlockOffset = new long[numberOfKeyBlockInfo];
            for (int i = 0; i < numberOfKeyBlockInfo; i++) {
                keyIndexBlockLengths[i] = buffer.getInt();
                keyIndexBlockOffset[i] = buffer.getLong();
            }
            int numberofAggKeyBlocks = buffer.getInt();
            int[] dataIndexMapLength = new int[numberofAggKeyBlocks];
            long[] dataIndexMapOffsets = new long[numberofAggKeyBlocks];
            for (int i = 0; i < numberofAggKeyBlocks; i++) {
                dataIndexMapLength[i] = buffer.getInt();
                dataIndexMapOffsets[i] = buffer.getLong();
            }
            info.setKeyBlockIndexLength(keyIndexBlockLengths);
            info.setKeyBlockIndexOffSets(keyIndexBlockOffset);
            info.setDataIndexMapLength(dataIndexMapLength);
            info.setDataIndexMapOffsets(dataIndexMapOffsets);
            info.setMeasureLength(msrLength);
            info.setMeasureOffset(msrOffset);
            //            info.setAggKeyBlock(isUniqueBlock);
            listOfNodeInfo.add(info);
        }
        fileHolder.finish();
        return listOfNodeInfo;
    }

    /**
     * @param buffer
     * @param info
     * @param startKey
     * @param endKey
     * @param keySplitValue
     */
    public static void setInfo(ByteBuffer buffer, LeafNodeInfoColumnar info, byte[] startKey,
            byte[] endKey, int keySplitValue) {
        int[] keyLengths = new int[keySplitValue];
        long[] keyOffset = new long[keySplitValue];
        boolean[] isAlreadySorted = new boolean[keySplitValue];
        for (int i = 0; i < keySplitValue; i++) {
            keyLengths[i] = buffer.getInt();
            keyOffset[i] = buffer.getLong();
            isAlreadySorted[i] = buffer.get() == (byte) 0 ? true : false;
        }
        //read column min max data
        byte[][] columnMinMaxData = new byte[buffer.getInt()][];
        for (int i = 0; i < columnMinMaxData.length; i++) {
            columnMinMaxData[i] = new byte[buffer.getInt()];
            buffer.get(columnMinMaxData[i]);
        }
        info.setColumnMinMaxData(columnMinMaxData);

        info.setKeyLengths(keyLengths);
        info.setKeyOffSets(keyOffset);
        info.setIsSortedKeyColumn(isAlreadySorted);
        buffer.get(startKey);
        //
        buffer.get(endKey);
        info.setStartKey(startKey);
    }

    /**
     * This method will be used to read the slice metadata
     *
     * @param rsFiles
     * @return slice meta data
     * @throws MolapUtilException
     */
    public static SliceMetaData readSliceMetadata(File rsFiles, int restructFolderNumber)
            throws MolapUtilException {
        SliceMetaData readObject = null;
        InputStream stream = null;
        ObjectInputStream objectInputStream = null;
        File file = null;
        try {
            file = new File(
                    rsFiles + File.separator + getSliceMetaDataFileName(restructFolderNumber));
            stream = new FileInputStream(
                    rsFiles + File.separator + getSliceMetaDataFileName(restructFolderNumber));
            objectInputStream = new ObjectInputStream(stream);
            readObject = (SliceMetaData) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new MolapUtilException(
                    "Problem while reading the slicemeta data file " + file.getAbsolutePath(), e);
        }
        //
        catch (IOException e) {
            throw new MolapUtilException("Problem while reading the slicemeta data file ", e);
        } finally {
            MolapUtil.closeStreams(objectInputStream, stream);
        }
        return readObject;
    }

    public static void writeSliceMetaDataFile(String path, SliceMetaData sliceMetaData,
            int nextRestructFolder) {
        OutputStream stream = null;
        ObjectOutputStream objectOutputStream = null;
        try {
            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Slice Metadata file Path: " + path + '/' + MolapUtil
                            .getSliceMetaDataFileName(nextRestructFolder));
            stream = FileFactory.getDataOutputStream(
                    path + File.separator + MolapUtil.getSliceMetaDataFileName(nextRestructFolder),
                    FileFactory.getFileType(path));
            objectOutputStream = new ObjectOutputStream(stream);
            objectOutputStream.writeObject(sliceMetaData);
        } catch (IOException e) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e.getMessage());
        } finally {
            MolapUtil.closeStreams(objectOutputStream, stream);
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

    public static void deleteFiles(File[] intermediateFiles) throws MolapUtilException {
        for (int i = 0; i < intermediateFiles.length; i++) {
            if (!intermediateFiles[i].delete()) {
                throw new MolapUtilException("Problem while deleting intermediate file");
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
                            && pathname.getName().endsWith(MolapCommonConstants.FACT_FILE_EXT));

                }
            });
        }
        return files;
    }

    /**
     * This method will be used to for sending the new slice signal to engine
     */
    public static void flushSEQGenLruCache() {
        try {
            // inform engine to load new slice
            Class<?> c =
                    Class.forName("com.huawei.unibi.molap.surrogatekeysgenerator.lru.LRUCache");
            Class[] argTypes = new Class[] {};
            // get the instance of CubeSliceLoader
            Method main = c.getDeclaredMethod("getIntance", argTypes);
            Object invoke = main.invoke(null, null);

            // ionvoke loadSliceFromFile
            Method declaredMethod = c.getDeclaredMethod("flush");
            // pass cube name and store location
            declaredMethod.invoke(invoke);
        } catch (ClassNotFoundException classNotFoundException) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while clearing the cache " + classNotFoundException);
        } catch (NoSuchMethodException noSuchMethodException) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while clearing the cache " + noSuchMethodException);
        } catch (IllegalAccessException illegalAccessException) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while clearing the cache " + illegalAccessException);
        } catch (InvocationTargetException invocationTargetException) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while clearing the cache " + invocationTargetException);
        }
    }

    /**
     * This method will be used to for sending the new slice signal to engine
     */
    public static void flushSEQGenLruCache(String key) throws MolapUtilException {
        try {
            // inform engine to load new slice
            Class<?> c =
                    Class.forName("com.huawei.unibi.molap.surrogatekeysgenerator.lru.LRUCache");
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
            throw new MolapUtilException("Problem while flushin the seqgen lru cache",
                    classNotFoundException);
        } catch (NoSuchMethodException noSuchMethodException) {
            throw new MolapUtilException("Problem while flushin the seqgen lru cache",
                    noSuchMethodException);
        } catch (IllegalAccessException illegalAccessException) {
            throw new MolapUtilException("Problem while flushin the seqgen lru cache",
                    illegalAccessException);
        } catch (InvocationTargetException invocationTargetException) {
            throw new MolapUtilException("Problem while flushin the seqgen lru cache",
                    invocationTargetException);
        }
    }

    public static short[] getUnCompressColumnIndex(int totalLength, byte[] columnIndexData) {
        ByteBuffer buffer = ByteBuffer.wrap(columnIndexData);
        buffer.rewind();
        int indexDataLength = buffer.getInt();
        short[] indexData = new short[indexDataLength / 2];
        short[] indexMap =
                new short[(totalLength - indexDataLength - MolapCommonConstants.INT_SIZE_IN_BYTE)
                        / 2];
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
    
    public static ColumnarKeyStoreInfo getColumnarKeyStoreInfo(LeafNodeInfoColumnar leafNodeInfo, int[] eachBlockSize,HybridStoreModel hybridStoreMeta)
    {
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
                MolapProperties.getInstance().getProperty(MolapCommonConstants.LEAFNODE_SIZE,
                        MolapCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL))));
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
            blockKeySize =
                    columnarKeyStoreDataHolder[i].getColumnarKeyStoreMetadata().getEachRowSize();
            if (columnarKeyStoreDataHolder[i].getColumnarKeyStoreMetadata().isSorted()) {
                for (int j = 0; j < keyBlockData.length; j += blockKeySize) {
                    System.arraycopy(keyBlockData, j, completeKeyArray, destinationPosition,
                            blockKeySize);
                    destinationPosition += eachKeySize;
                }
            } else {
                columnIndex = columnarKeyStoreDataHolder[i].getColumnarKeyStoreMetadata()
                        .getColumnIndex();

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
            blockKeySize =
                    columnarKeyStoreDataHolder[i].getColumnarKeyStoreMetadata().getEachRowSize();

            for (int j = 0; j < columnIndex.length; j++) {
                System.arraycopy(keyBlockData, columnIndex[j] * blockKeySize, completeKeyArray,
                        destinationPosition, blockKeySize);
                destinationPosition += eachKeySize;
            }
            destinationPosition = blockKeySize;
        }
        return completeKeyArray;
    }

    public static int getLastIndexUsingBinarySearch(ColumnarKeyStoreDataHolder keyBlockArray,
            int low, int high, byte[] compareValue, int numberOfRows) {
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

    public static int getFirstIndexUsingBinarySearch(ColumnarKeyStoreDataHolder keyBlockArray,
            int low, int high, byte[] compareValue) {
        int cmpResult = 0;
        while (high >= low) {
            int mid = (low + high) / 2;
            cmpResult = ByteUtil.UnsafeComparer.INSTANCE
                    .compareTo(keyBlockArray.getKeyBlockData(), mid * compareValue.length,
                            compareValue.length, compareValue, 0, compareValue.length);
            if (cmpResult < 0) {
                low = mid + 1;
            } else if (cmpResult > 0) {
                high = mid - 1;
            } else {
                int currentIndex = mid;
                while (currentIndex - 1 >= 0 && ByteUtil.UnsafeComparer.INSTANCE
                        .compareTo(keyBlockArray.getKeyBlockData(),
                                (currentIndex - 1) * compareValue.length, compareValue.length,
                                compareValue, 0, compareValue.length) == 0) {
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
                    .compareTo(keyBlockArray.getKeyBlockData(), (mid - 1) * length, length,
                            compareValue, offset, length) < 0)) {
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
                    .compareTo(keyBlockArray.getKeyBlockData(), mid * lenghtToCompare,
                            lenghtToCompare, compareValue, offset, lenghtToCompare);
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

    public static int byteArrayBinarySearch(byte[][] filter,
            ColumnarKeyStoreDataHolder keyBlockArray, int index) {
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
            indexMap =
                    new byte[totalLength - indexDataLength - MolapCommonConstants.INT_SIZE_IN_BYTE];
            buffer.get(indexData);
            buffer.get(indexMap);
        } catch (Exception e) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, "Error while compressColumn Index ",
                    e, e.getMessage());
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

    public static String[] getSlices(String storeLocation, String tableName, FileType fileType) {
        try {
            if (!FileFactory.isFileExist(storeLocation, fileType)) {
                return new String[0];
            }
        } catch (IOException e) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error occurred :: " + e.getMessage());
        }
        MolapFile file = FileFactory.getMolapFile(storeLocation, fileType);
        MolapFile[] listFiles = listFiles(file);
        if (null == listFiles || listFiles.length < 0) {
            return new String[0];
        }
        Arrays.sort(listFiles, new MolapFileFolderComparator());
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
    public static MolapFile[] listFiles(MolapFile file) {
        MolapFile[] listFiles = file.listFiles(new MolapFileFilter() {
            @Override public boolean accept(MolapFile pathname) {
                return pathname.getName().startsWith(MolapCommonConstants.LOAD_FOLDER) && !pathname
                        .getName().endsWith(MolapCommonConstants.FILE_INPROGRESS_STATUS);
            }
        });
        return listFiles;
    }

    public static List<MolapSliceAndFiles> getSliceAndFilesList(String tableName,
            MolapFile[] listFiles, FileType fileType) {

        List<MolapSliceAndFiles> sliceFactFilesList =
                new ArrayList<MolapSliceAndFiles>(listFiles.length);
        if (listFiles.length == 0) {
            return sliceFactFilesList;
        }

        MolapSliceAndFiles sliceAndFiles = null;
        MolapFile[] sortedPathForFiles = null;
        for (int i = 0; i < listFiles.length; i++) {
            sliceAndFiles = new MolapSliceAndFiles();
            sliceAndFiles.setPath(listFiles[i].getAbsolutePath());
            sortedPathForFiles = getAllFactFiles(sliceAndFiles.getPath(), tableName, fileType);
            if (null != sortedPathForFiles && sortedPathForFiles.length > 0) {
                Arrays.sort(sortedPathForFiles,
                        new MolapFileComparator("\\" + MolapCommonConstants.FACT_FILE_EXT));
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
    public static MolapFile[] getAllFactFiles(String sliceLocation, final String tableName,
            FileType fileType) {
        MolapFile file = FileFactory.getMolapFile(sliceLocation, fileType);
        MolapFile[] files = null;
        MolapFile[] updatedFactFiles = null;
        if (file.isDirectory()) {
            updatedFactFiles = file.listFiles(new MolapFileFilter() {

                @Override public boolean accept(MolapFile pathname) {
                    return ((!pathname.isDirectory()) && (pathname.getName().startsWith(tableName))
                            && pathname.getName()
                            .endsWith(MolapCommonConstants.FACT_UPDATE_EXTENSION));
                }
            });

            if (updatedFactFiles.length != 0) {
                return updatedFactFiles;

            }

            files = file.listFiles(new MolapFileFilter() {
                public boolean accept(MolapFile pathname) {
                    return ((!pathname.isDirectory()) && (pathname.getName().startsWith(tableName))
                            && pathname.getName().endsWith(MolapCommonConstants.FACT_FILE_EXT));

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
     * @throws MolapUtilException
     */
    public static int[] getCardinalityFromLevelMetadataFile(String levelPath)
            throws MolapUtilException {
        DataInputStream dataInputStream = null;
        int[] cardinality = null;

        try {
            if (FileFactory.isFileExist(levelPath, FileFactory.getFileType(levelPath))) {
                dataInputStream = FileFactory
                        .getDataInputStream(levelPath, FileFactory.getFileType(levelPath));

                cardinality = new int[dataInputStream.readInt()];

                for (int i = 0; i < cardinality.length; i++) {
                    cardinality[i] = dataInputStream.readInt();
                }
            }
        } catch (FileNotFoundException e) {
            throw new MolapUtilException("Problem while getting the file", e);
        } catch (IOException e) {
            throw new MolapUtilException("Problem while reading the file", e);
        } finally {
            MolapUtil.closeStreams(dataInputStream);
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
            count = Integer.parseInt(highestCountAggTableName
                    .substring(highestCountAggTableName.lastIndexOf("_") + 1)) + count;
        }
        return MolapCommonConstants.AGGREGATE_TABLE_START_TAG + MolapCommonConstants.UNDERSCORE
                + factTableName + MolapCommonConstants.UNDERSCORE + count;
    }

    public static String getRSPath(String schemaName, String cubeName, String tableName,
            String hdfsLocation, int currentRestructNumber) {
        if (null == hdfsLocation) {
            hdfsLocation = MolapProperties.getInstance()
                    .getProperty(MolapCommonConstants.STORE_LOCATION_HDFS);
        }

        String hdfsStoreLocation = hdfsLocation;
        hdfsStoreLocation =
                hdfsStoreLocation + File.separator + schemaName + File.separator + cubeName;

        int rsCounter = currentRestructNumber/*MolapUtil.checkAndReturnNextRestructFolderNumber(
                hdfsStoreLocation, "RS_")*/;
        if (rsCounter == -1) {
            rsCounter = 0;
        }
        String hdfsLoadedTable =
                hdfsStoreLocation + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER
                        + rsCounter + "/" + tableName;
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
        String metadataFileName = cubeFolderPath + MolapCommonConstants.FILE_SEPARATOR
                + MolapCommonConstants.LOADMETADATA_FILENAME
                + MolapCommonConstants.MOLAP_METADATA_EXTENSION;
        LoadMetadataDetails[] listOfLoadFolderDetailsArray;

        AtomicFileOperations fileOperation = new AtomicFileOperationsImpl(metadataFileName,
                FileFactory.getFileType(metadataFileName));

        try {
            if (!FileFactory
                    .isFileExist(metadataFileName, FileFactory.getFileType(metadataFileName))) {
                return new LoadMetadataDetails[0];
            }
            dataInputStream = fileOperation.openForRead();
            inStream = new InputStreamReader(dataInputStream,
                    MolapCommonConstants.MOLAP_DEFAULT_STREAM_ENCODEFORMAT);
            buffReader = new BufferedReader(inStream);
            listOfLoadFolderDetailsArray =
                    gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
        } catch (IOException e) {
            return new LoadMetadataDetails[0];
        } finally {
            MolapUtil.closeStreams(buffReader, inStream, dataInputStream);
        }

        return listOfLoadFolderDetailsArray;
    }

    public static boolean createRSMetaFile(String metaDataPath, String newRSFileName) {
        String fullFileName = metaDataPath + File.separator + newRSFileName;
        FileType fileType = FileFactory.getFileType(metaDataPath + File.separator + newRSFileName);
        try {
            return FileFactory.createNewFile(fullFileName, fileType);
        } catch (IOException e) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while writing RS meta file : " + fullFileName + e.getMessage());
            return false;
        }
    }

    public static String getSliceMetaDataFileName(int restructFolderNumber) {
        return MolapCommonConstants.SLICE_METADATA_FILENAME + "." + restructFolderNumber;
    }

    public static void writeLevelCardinalityFile(String loadFolderLoc, String tableName,
            int[] dimCardinality) throws KettleException {
        String levelCardinalityFilePath = loadFolderLoc + File.separator +
                MolapCommonConstants.LEVEL_METADATA_FILE + tableName + ".metadata";

        FileOutputStream fileOutputStream = null;
        FileChannel channel = null;
        try {
            int dimCardinalityArrLength = dimCardinality.length;

            // first four bytes for writing the length of array, remaining for array data
            ByteBuffer buffer = ByteBuffer.allocate(MolapCommonConstants.INT_SIZE_IN_BYTE
                    + dimCardinalityArrLength * MolapCommonConstants.INT_SIZE_IN_BYTE);

            fileOutputStream = new FileOutputStream(levelCardinalityFilePath);
            channel = fileOutputStream.getChannel();
            buffer.putInt(dimCardinalityArrLength);

            for (int i = 0; i < dimCardinalityArrLength; i++) {
                buffer.putInt(dimCardinality[i]);
            }

            buffer.flip();
            channel.write(buffer);
            buffer.clear();

            LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Level cardinality file written to : " + levelCardinalityFilePath);
        } catch (IOException e) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "Error while writing level cardinality file : " + levelCardinalityFilePath + e
                            .getMessage());
            throw new KettleException("Not able to write level cardinality file", e);
        } finally {
            MolapUtil.closeStreams(channel, fileOutputStream);
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
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e);
        } catch (FileNotFoundException e) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "@@@@@ SliceMetaData File is missing @@@@@ :" + path);
        } catch (IOException e) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG,
                    "@@@@@ Error while reading SliceMetaData File @@@@@ :" + path);
        } finally {
            MolapUtil.closeStreams(objectInputStream, stream);
        }
        return readObject;
    }

    public static SliceMetaData readSliceMetaDataFile(String folderPath,
            int currentRestructNumber) {
        String path = folderPath + '/' + MolapUtil.getSliceMetaDataFileName(currentRestructNumber);
        return readSliceMetaDataFile(path);
    }

    public static SliceMetaData readSliceMetaDataFile(MolapFile folderPath) {
        MolapFile[] sliceMetaDataPath = folderPath.listFiles(new MolapFileFilter() {

            @Override public boolean accept(MolapFile file) {
                return file.getName().startsWith("sliceMetaData");
            }
        });

        if (null == sliceMetaDataPath || sliceMetaDataPath.length < 1) {
            return null;
        }
        Arrays.sort(sliceMetaDataPath, new SliceMetaDataFileComparator());
        return readSliceMetaDataFile(
                sliceMetaDataPath[sliceMetaDataPath.length - 1].getAbsolutePath());
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
                FileFactory.getFileType(filePath) != FileType.HDFS) {
            String baseHDFSUrl = MolapProperties.getInstance().
                    getProperty(MolapCommonConstants.CARBON_DDL_BASE_HDFS_URL);
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
                location.substring(location.indexOf(MolapCommonConstants.RESTRUCTRE_FOLDER));
        int factTableIndex = restructName.indexOf(factTableName) - 1;
        String restructNumber = restructName
                .substring(MolapCommonConstants.RESTRUCTRE_FOLDER.length(), factTableIndex);
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
            retryInterval = Long.parseLong(MolapProperties.getInstance()
                    .getProperty(MolapCommonConstants.CARBON_LOAD_LEVEL_RETRY_INTERVAL,
                            MolapCommonConstants.CARBON_LOAD_LEVEL_RETRY_INTERVAL_DEFAULT));
        } catch (NumberFormatException e) {
            retryInterval = Long.parseLong(MolapProperties.getInstance()
                    .getProperty(MolapCommonConstants.CARBON_LOAD_LEVEL_RETRY_INTERVAL_DEFAULT));
        }
        retryInterval = retryInterval * 1000;
        return retryInterval;
    }

    /**
     * Below method will be used to get the aggregator type
     * MolapCommonConstants.SUM_COUNT_VALUE_MEASURE will return when value is double measure
     * MolapCommonConstants.BYTE_VALUE_MEASURE will be returned when value is byte array
     *
     * @param agg
     * @return aggregator type
     */
    public static char getType(String agg) {
        if (MolapCommonConstants.SUM.equals(agg) || MolapCommonConstants.COUNT.equals(agg)) {
            return MolapCommonConstants.SUM_COUNT_VALUE_MEASURE;
        } else {
            return MolapCommonConstants.BYTE_VALUE_MEASURE;
        }
    }

    public static String getCarbonStorePath(String schemaName, String cubeName) {
        MolapProperties prop = MolapProperties.getInstance();
        if (null == prop) {
            return null;
        }
        String basePath = prop.getProperty(MolapCommonConstants.STORE_LOCATION,
                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL);
        String useUniquePath = MolapProperties.getInstance()
                .getProperty(MolapCommonConstants.CARBON_UNIFIED_STORE_PATH,
                        MolapCommonConstants.CARBON_UNIFIED_STORE_PATH_DEFAULT);
        if (null != schemaName && !schemaName.isEmpty() && null != cubeName && !cubeName.isEmpty()
                && "true".equals(useUniquePath)) {
            basePath = basePath + File.separator + schemaName + File.separator + cubeName;
        }
        return basePath;
    }

    /**
     * Thread to delete the cubes
     *
     * @author m00258959
     */
    private static final class DeleteCube implements Callable<Void> {
        private MolapFile file;

        private DeleteCube(MolapFile file) {
            this.file = file;
        }

        @Override public Void call() throws Exception {
            deleteFoldersAndFiles(file);
            return null;
        }

    }

    private static class MolapFileComparator implements Comparator<MolapFile> {
        /**
         * File extension
         */
        private String fileExt;

        public MolapFileComparator(String fileExt) {
            this.fileExt = fileExt;
        }

        @Override public int compare(MolapFile file1, MolapFile file2) {
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
            int index1 = aggTable1.lastIndexOf(MolapCommonConstants.UNDERSCORE);
            int index2 = aggTable2.lastIndexOf(MolapCommonConstants.UNDERSCORE);
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

    private static class SliceMetaDataFileComparator implements Comparator<MolapFile> {

        @Override public int compare(MolapFile o1, MolapFile o2) {
            int firstSliceNumber = Integer.parseInt(o1.getName().split("\\.")[1]);
            int secondSliceNumber = Integer.parseInt(o2.getName().split("\\.")[1]);
            return firstSliceNumber - secondSliceNumber;
        }

    }
}

