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

package org.carbondata.query.util;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.commons.codec.binary.Base64;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.MolapFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.metadata.MolapMetadata.Dimension;
import org.carbondata.core.metadata.MolapSchemaReader;
import org.carbondata.core.olap.MolapDef;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.query.datastorage.Member;
import org.carbondata.query.queryinterface.filter.MolapFilterInfo;
import org.carbondata.query.wrappers.ArrayWrapper;

//import org.pentaho.platform.util.logging.Logger;

/**
 * Class Description : This class will be used for prepare member and
 * hierarchies cache , it will read member and hierarchies files to prepare
 * cache Dimension: Filename = DimensionName Columns = 2 Rows =
 * Value(String),SurrogateKey(Long) Hierarchy: Filename= HierachyName Content =
 * byte content Fact/Aggregate: Filename= TableName Columns = dimensions +
 * measures Content = ; separated values
 * Version 1.0
 */

public final class CacheUtil {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CacheUtil.class.getName());
    /**
     * Map intiaL capacity
     */
    private static final int SET_INTIAL_CAPECITY = 1000;
    /**
     * list initial capacity
     */
    private static final int LIST_INTIAL_CAPECITY = 10;

    private CacheUtil() {

    }

    /**
     * This method Read dimension files and prepare Int2ObjectMap for dimension
     * which will contain all the member of dimension
     * Dimension: Filename = DimensionName Columns = 2 Rows =
     * Value(String),SurrogateKey(Long)
     *
     * @param filesLocaton    dimension file location
     * @param namecolumnindex name column index
     * @return Int2ObjectMap<Member> it will contain all the members
     */
    public static Member[][] getMembersList(String filesLocaton, byte nameColumnIndex,
            String dataType) {
        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "Reading members data from location: " + filesLocaton);
        //        File decryptedFile = decryptEncyptedFile(filesLocaton);
        Member[][] members = processMemberFile(filesLocaton, dataType);
        return members;
    }

    public static int getMinValue(String filesLocaton) {
        if (null == filesLocaton) {
            return 0;
        }
        DataInputStream fileChannel = null;
        try {
            if (!FileFactory.isFileExist(filesLocaton, FileFactory.getFileType(filesLocaton))) {
                return 0;
            }
            fileChannel = new DataInputStream(FileFactory
                    .getDataInputStream(filesLocaton, FileFactory.getFileType(filesLocaton),
                            10240));
            fileChannel.readInt();
            return fileChannel.readInt();
        } catch (IOException e) {
            //            e.printStackTrace();
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
        } finally {
            MolapUtil.closeStreams(fileChannel);
        }
        return 0;
    }

    public static int getMinValueFromLevelFile(String filesLocaton) {
        if (null == filesLocaton) {
            return 0;
        }
        DataInputStream fileChannel = null;
        try {
            if (!FileFactory.isFileExist(filesLocaton, FileFactory.getFileType(filesLocaton))) {
                return 0;
            }
            fileChannel = new DataInputStream(FileFactory
                    .getDataInputStream(filesLocaton, FileFactory.getFileType(filesLocaton),
                            10240));
            return fileChannel.readInt();

        } catch (IOException e) {
            //            e.printStackTrace();
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
        } finally {
            MolapUtil.closeStreams(fileChannel);
        }
        return 0;
    }

    public static int getMaxValueFromLevelFile(String filesLocaton) {
        if (null == filesLocaton) {
            return 0;
        }
        DataInputStream fileChannel = null;
        try {
            if (!FileFactory.isFileExist(filesLocaton, FileFactory.getFileType(filesLocaton))) {
                return 0;
            }
            fileChannel = new DataInputStream(FileFactory
                    .getDataInputStream(filesLocaton, FileFactory.getFileType(filesLocaton),
                            10240));
            MolapFile memberFile =
                    FileFactory.getMolapFile(filesLocaton, FileFactory.getFileType(filesLocaton));
            long size = memberFile.getSize() - 4;
            long skipSize = size;
            long actualSkipSize = 0;
            while (actualSkipSize != size) {
                actualSkipSize += fileChannel.skip(skipSize);
                skipSize = skipSize - actualSkipSize;
            }
            LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Bytes skipped " + skipSize);
            int maxVal = fileChannel.readInt();
            return maxVal;

        } catch (IOException e) {
            //            e.printStackTrace();
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
        } finally {
            MolapUtil.closeStreams(fileChannel);
        }
        return 0;
    }

    public static int[] getGlobalSurrogateMapping(String filesLocaton) {
        int[] globalMapping = new int[0];
        if (null == filesLocaton) {
            return globalMapping;
        }
        DataInputStream fileChannel = null;
        try {
            if (!FileFactory.isFileExist(filesLocaton, FileFactory.getFileType(filesLocaton))) {
                return null;
            }
            fileChannel = FileFactory
                    .getDataInputStream(filesLocaton, FileFactory.getFileType(filesLocaton));
            fileChannel.readInt();
            int minValue = fileChannel.readInt();
            int numberOfEntries = fileChannel.readInt();
            globalMapping = new int[numberOfEntries];
            int counter = 0;
            int index = 0;
            while (counter < numberOfEntries) {
                index = fileChannel.readInt();
                globalMapping[index - minValue] = fileChannel.readInt();
                counter++;
            }
        } catch (IOException e) {
            //            e.printStackTrace();
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
        } finally {
            MolapUtil.closeStreams(fileChannel);
        }
        return globalMapping;
    }

    public static Map<Integer, Integer> getGlobalSurrogateMappingMapBased(String filesLocaton) {
        if (null == filesLocaton) {
            return null;
        }
        DataInputStream fileChannel = null;
        Map<Integer, Integer> map =
                new HashMap<Integer, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        try {
            if (!FileFactory.isFileExist(filesLocaton, FileFactory.getFileType(filesLocaton))) {
                return null;
            }
            fileChannel = FileFactory
                    .getDataInputStream(filesLocaton, FileFactory.getFileType(filesLocaton));
            fileChannel.readInt();
            int numberOfEntries = fileChannel.readInt();
            int counter = 0;
            while (counter < numberOfEntries) {
                map.put(fileChannel.readInt(), fileChannel.readInt());
                counter++;
            }
        } catch (IOException e) {
            //            e.printStackTrace();
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
        } finally {
            MolapUtil.closeStreams(fileChannel);
        }
        return map;
    }

    /**
     *
     * @param memberFile
     * @param inProgressLoadFolder
     * @return
     * @throws KettleException
     *
     */
    //    private static File decryptEncyptedFile(String memberFile)
    //    {
    //        String decryptedFilePath = memberFile + MolapCommonConstants.FILE_INPROGRESS_STATUS;
    //
    //        try
    //        {
    //            SimpleFileEncryptor.decryptFile(memberFile , decryptedFilePath);
    //        }
    //        catch(CipherException e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Error while decrypting File");
    //        }
    //        catch(IOException e)
    //        {
    //            LOGGER.error(
    //                    MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                    e, "Not able to encrypt File");
    //        }
    //
    //        return new File(decryptedFilePath);
    //    }

    /**
     * @param nameColumnIndex
     * @param filename
     * @param members
     * @throws IOException
     * @author A00902732
     * </br>     Reads the member file
     * </br>
     * Member file structure
     * </br>           |1|2|3|4|5|6|7|8....
     * </br>           |1|2|3|4|5|6|....
     * </br>
     * </br>            1#totalRecoredLength - exclusive - 4 bytes
     * </br>            2#valueLength - 4 bytes
     * </br>            3#Value - depends on previous length
     * </br>            4#surrogateKey - 4 bytes
     * </br>            5#property1Length - 4 bytes
     * </br>            6#property1Value - depends on prev length
     * </br>            7#property2Length - 4 bytes
     * </br>            8#property2Value - depends on prev length
     * </br>            ...
     * Reads accordingly
     */
    public static Member[][] processMemberFile(String filename, String dataType)

    {
        long startTime = System.currentTimeMillis();
        Member[][] members = null;
        // create an object of FileOutputStream
        DataInputStream fileChannel = null;
        try {
            try {
                if (!FileFactory.isFileExist(filename, FileFactory.getFileType(filename))) {
                    return members;
                }
            } catch (IOException e) {
                return members;
            }
            //
            fileChannel =
                    FileFactory.getDataInputStream(filename, FileFactory.getFileType(filename));
            FileType fileType = FileFactory.getFileType(filename);
            MolapFile memberFile = FileFactory.getMolapFile(filename, fileType);
            members = populateMemberCache(fileChannel, memberFile, filename, dataType);
        } catch (FileNotFoundException f) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "@@@@@@  Member file is missing @@@@@@ : " + filename);
        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "@@@@@@  Error while reading Member the file @@@@@@ : " + filename);
        } finally {
            MolapUtil.closeStreams(fileChannel);
        }

        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "Time taken to process file " + filename + " is : " + (System.currentTimeMillis()
                        - startTime));
        return members;
    }

    public static void processMemberFileNewImpl(int nameColumnIndex, String filename,
            Int2ObjectMap<Member> members, String dataType)

    {
        long startTime = System.currentTimeMillis();
        // Coverity Fix add null check
        if (null == filename || null == members) {
            return;
        }
        // create an object of FileOutputStream
        DataInputStream fileChannel = null;
        try {
            //
            try {
                if (!FileFactory.isFileExist(filename, FileFactory.getFileType(filename))) {
                    return;
                }
            } catch (IOException e) {
                return;
            }
            FileType fileType = FileFactory.getFileType(filename);
            MolapFile memberFile = FileFactory.getMolapFile(filename, fileType);

            fileChannel =
                    FileFactory.getDataInputStream(filename, FileFactory.getFileType(filename));
            populateMemberCache(fileChannel, memberFile, filename, dataType);
        } catch (FileNotFoundException f) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "@@@@@@  Member file is missing @@@@@@ : " + filename);
        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "@@@@@@  Error while reading Member the file @@@@@@ : " + filename);
        } finally {
            MolapUtil.closeStreams(fileChannel);
        }

        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "Time taken to process file " + filename + " is : " + (System.currentTimeMillis()
                        - startTime));

    }

    private static Member[][] populateMemberCache(DataInputStream fileChannel, MolapFile memberFile,
            String fileName, String dataType) throws IOException {
        // ByteBuffer toltalLength, memberLength, surrogateKey, bf3;
        // subtracted 4 as last 4 bytes will have the max value for no of
        // records
        long currPositionIndex = 0;
        long size = memberFile.getSize() - 4;
        long skipSize = size;
        long actualSkipSize = 0;
        while (actualSkipSize != size) {
            actualSkipSize += fileChannel.skip(skipSize);
            skipSize = skipSize - actualSkipSize;
        }
        //        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Bytes skipped " + skipSize);
        int maxVal = fileChannel.readInt();
        MolapUtil.closeStreams(fileChannel);
        fileChannel = FileFactory.getDataInputStream(fileName, FileFactory.getFileType(fileName));
        // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
        ByteBuffer buffer = ByteBuffer.allocate((int) size);
        // CHECKSTYLE:OFF
        fileChannel.readFully(buffer.array());
        int minVal = buffer.getInt();
        int totalArraySize = maxVal - minVal + 1;
        Member[][] surogateKeyArrays = null;
        if (totalArraySize > MolapCommonConstants.LEVEL_ARRAY_SIZE) {
            int div = totalArraySize / MolapCommonConstants.LEVEL_ARRAY_SIZE;
            int rem = totalArraySize % MolapCommonConstants.LEVEL_ARRAY_SIZE;
            if (rem > 0) {
                div++;
            }
            surogateKeyArrays = new Member[div][];

            for (int i = 0; i < div - 1; i++) {
                surogateKeyArrays[i] = new Member[MolapCommonConstants.LEVEL_ARRAY_SIZE];
            }

            if (rem > 0) {
                surogateKeyArrays[surogateKeyArrays.length - 1] = new Member[rem];
            } else {
                surogateKeyArrays[surogateKeyArrays.length - 1] =
                        new Member[MolapCommonConstants.LEVEL_ARRAY_SIZE];
            }
        } else {
            surogateKeyArrays = new Member[1][totalArraySize];
        }
        //        Member[] surogateKeyArrays = new Member[maxVal-minVal+1];
        //        int surrogateKeyIndex = minVal;
        currPositionIndex += 4;
        //
        int current = 0;
        // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
        boolean enableEncoding = Boolean.valueOf(MolapProperties.getInstance()
                .getProperty(MolapCommonConstants.ENABLE_BASE64_ENCODING,
                        MolapCommonConstants.ENABLE_BASE64_ENCODING_DEFAULT));
        // CHECKSTYLE:ON
        int index = 0;
        int prvArrayIndex = 0;
        while (currPositionIndex < size) {
            int len = buffer.getInt();
            // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
            // CHECKSTYLE:ON
            currPositionIndex += 4;
            byte[] rowBytes = new byte[len];
            buffer.get(rowBytes);
            currPositionIndex += len;
            // No:Approval-361
            if (enableEncoding) {
                rowBytes = Base64.decodeBase64(rowBytes);
            }
            surogateKeyArrays[current / MolapCommonConstants.LEVEL_ARRAY_SIZE][index] =
                    new Member(rowBytes);
            current++;
            if (current / MolapCommonConstants.LEVEL_ARRAY_SIZE > prvArrayIndex) {
                prvArrayIndex++;
                index = 0;
            } else {
                index++;
            }
        }
        return surogateKeyArrays;
    }

    /**
     * Below method is responsible for reading the sort index and sort reverse index
     *
     * @param levelFileName
     * @return List<int[][]>
     * @throws IOException
     */
    public static List<int[][]> getLevelSortOrderAndReverseIndex(String levelFileName)
            throws IOException {
        if (!FileFactory.isFileExist(levelFileName + MolapCommonConstants.LEVEL_SORT_INDEX_FILE_EXT,
                FileFactory.getFileType(
                        levelFileName + MolapCommonConstants.LEVEL_SORT_INDEX_FILE_EXT))) {
            return null;
        }
        DataInputStream dataInputStream = null;
        List<int[][]> sortIndexAndReverseIndexArray =
                new ArrayList<int[][]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        long size = 0;
        ByteBuffer buffer = null;
        try {
            dataInputStream = FileFactory.getDataInputStream(
                    levelFileName + MolapCommonConstants.LEVEL_SORT_INDEX_FILE_EXT,
                    FileFactory.getFileType(levelFileName));
            size = FileFactory
                    .getMolapFile(levelFileName + MolapCommonConstants.LEVEL_SORT_INDEX_FILE_EXT,
                            FileFactory.getFileType(levelFileName)).getSize();
            // CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
            buffer = ByteBuffer.allocate((int) size);
            // CHECKSTYLE:ON
            dataInputStream.readFully(buffer.array());
            sortIndexAndReverseIndexArray.add(getIndexArray(buffer));
            sortIndexAndReverseIndexArray.add(getIndexArray(buffer));
        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            throw e;
        } finally {
            MolapUtil.closeStreams(dataInputStream);
        }
        return sortIndexAndReverseIndexArray;
    }

    private static int[][] getIndexArray(ByteBuffer buffer) {
        int arraySize = buffer.getInt();
        int[][] sortorderIndexArray = new int[1][arraySize];
        if (arraySize > MolapCommonConstants.LEVEL_ARRAY_SIZE) {
            int div = arraySize / MolapCommonConstants.LEVEL_ARRAY_SIZE;
            int rem = arraySize % MolapCommonConstants.LEVEL_ARRAY_SIZE;
            if (rem > 0) {
                div++;
            }
            sortorderIndexArray = new int[div][];
            for (int i = 0; i < div - 1; i++) {
                sortorderIndexArray[i] = new int[MolapCommonConstants.LEVEL_ARRAY_SIZE];
            }

            if (rem > 0) {
                sortorderIndexArray[sortorderIndexArray.length - 1] = new int[rem];
            } else {
                sortorderIndexArray[sortorderIndexArray.length - 1] =
                        new int[MolapCommonConstants.LEVEL_ARRAY_SIZE];
            }
        }
        int index = 0;
        int prvArrayIndex = 0;
        int current = 0;
        for (int i = 0; i < arraySize; i++) {
            sortorderIndexArray[current / MolapCommonConstants.LEVEL_ARRAY_SIZE][index] =
                    buffer.getInt();
            current++;
            if (current / MolapCommonConstants.LEVEL_ARRAY_SIZE > prvArrayIndex) {
                prvArrayIndex++;
                index = 0;
            } else {
                index++;
            }
        }
        return sortorderIndexArray;
    }

    /**
     * Read hierarchies from file. Hierarchies contain duplicate entries.
     * Hierarchy: Filename= HierachyName Content = byte content
     *
     * @param hName
     * @param filesLocaton
     * @param keyLength
     * @param keyGen
     * @return
     */
    public static List<long[]> getHierarchiesList(String filesLocaton, int keyLength,
            KeyGenerator keyGen) {
        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "Reading hierarchies from location: " + filesLocaton);

        // hierarchies list , this will hold hierarchy surrogate key
        List<long[]> hierarchies = new ArrayList<long[]>(LIST_INTIAL_CAPECITY);

        // set is used to remove duplicate entries
        Set<ArrayWrapper> wrapHiers = new HashSet<ArrayWrapper>(SET_INTIAL_CAPECITY);

        FileInputStream reader = null;
        try {
            reader = new FileInputStream(filesLocaton);
            // get key length
            byte[] line = new byte[keyLength];
            while (reader.read(line) == keyLength) {
                // add to set
                wrapHiers.add(new ArrayWrapper(keyGen.getKeyArray(line)));
            }
        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Error while reading the file: " + filesLocaton);
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                        "Error while closing the file stream for file " + filesLocaton);
            }
        }

        // copy all the hierarchies from set to list
        for (ArrayWrapper wrapHier : wrapHiers) {
            hierarchies.add(wrapHier.getData());
        }
        return hierarchies;
    }

    /**
     * This method is used to check whether any exclude filter is present in
     * constraints
     *
     * @param constraints Dimension and its filter info
     * @return true if exclude filter is present
     */
    public static boolean checkAnyExcludeExists(Map<Dimension, MolapFilterInfo> constraints) {
        for (Map.Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet()) {
            // check if filter size is greater than zero, if it is more than
            // zero than include filter is present
            if (entry.getValue().getExcludedMembers().size() > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method is used to check whether any include filter is present in
     * constraints
     *
     * @param constraints Dimension and its filter info
     * @return true if include filter is present
     */
    public static boolean checkAnyIncludeExists(Map<Dimension, MolapFilterInfo> constraints) {
        for (Map.Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet()) {
            // check if filter size is greater than zero, if it is more than
            // zero than include filter is present
            if (entry.getValue().getIncludedMembers().size() > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method is used to check whether any include filter is present in
     * constraints
     *
     * @param constraints Dimension and its filter info
     * @return true if include filter is present
     */
    public static boolean checkAnyIncludeOrExists(Map<Dimension, MolapFilterInfo> constraints) {
        for (Map.Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet()) {
            // check if filter size is greater than zero, if it is more than
            // zero than include filter is present
            if (entry.getValue().getIncludedOrMembers() != null
                    && entry.getValue().getIncludedOrMembers().size() > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * This method will return the size of a given file
     *
     * @param fileName
     * @return
     */
    public static long getMemberFileSize(String fileName) {
        if (isFileExists(fileName)) {
            FileType fileType = FileFactory.getFileType(fileName);
            MolapFile memberFile = FileFactory.getMolapFile(fileName, fileType);
            return memberFile.getSize();
        }
        return 0;
    }

    /**
     * @param fileName
     * @param fileType
     */
    public static boolean isFileExists(String fileName) {
        try {
            FileType fileType = FileFactory.getFileType(fileName);
            if (FileFactory.isFileExist(fileName, fileType)) {
                return true;
            }
        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "@@@@@@  Member file not found for size to be calculated @@@@@@ : " + fileName);
        }
        return false;
    }

    /**
     * This method will return the actual level name based on the dimension
     *
     * @param schema
     * @param dimension
     * @return
     */
    public static String getLevelActualName(MolapDef.Schema schema,
            MolapDef.CubeDimension dimension) {
        String levelActualName = null;
        org.carbondata.core.olap.MolapDef.Hierarchy[] extractHierarchies =
                MolapSchemaReader.extractHierarchies(schema, dimension);
        if (null != extractHierarchies) {
            for (org.carbondata.core.olap.MolapDef.Hierarchy hierarchy : extractHierarchies) {
                for (MolapDef.Level level : hierarchy.levels) {
                    levelActualName = level.column;
                    break;
                }
            }
        }
        return levelActualName;
    }

}
