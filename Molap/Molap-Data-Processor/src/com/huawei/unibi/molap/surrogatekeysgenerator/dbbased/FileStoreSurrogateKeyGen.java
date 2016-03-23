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

package com.huawei.unibi.molap.surrogatekeysgenerator.dbbased;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.*;
import java.util.Map.Entry;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.file.manager.composite.FileData;
import com.huawei.unibi.molap.file.manager.composite.IFileManagerComposite;
import com.huawei.unibi.molap.file.manager.composite.LoadFolderData;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.schema.metadata.MolapInfo;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.writer.LevelValueWriter;
import org.apache.commons.codec.binary.Base64;
import org.pentaho.di.core.exception.KettleException;

public class FileStoreSurrogateKeyGen extends MolapDimSurrogateKeyGen {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(FileStoreSurrogateKeyGen.class.getName());

    /**
     * dimensionWriter
     */
    private LevelValueWriter[] dimensionWriter;

    /**
     * hierValueWriter
     */
    private Map<String, HierarchyValueWriter> hierValueWriter;

    /**
     * keyGenerator
     */
    private Map<String, KeyGenerator> keyGeneratorMap;

    /**
     * baseStorePath
     */
    private String baseStorePath;

    /**
     * LOAD_FOLDER
     */
    private String loadFolderName;

    /**
     * folderList
     */
    private List<File> folderList = new ArrayList<File>(5);

    /**
     * File manager
     */
    private IFileManagerComposite fileManager;

    private int currentRestructNumber;

    /**
     * @param molapInfo
     * @throws KettleException
     */
    public FileStoreSurrogateKeyGen(MolapInfo molapInfo, int currentRestructNum)
            throws KettleException {
        super(molapInfo);

        keyGeneratorMap =
                new HashMap<String, KeyGenerator>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        baseStorePath = molapInfo.getBaseStoreLocation();

        String storeFolderWithLoadNumber =
                checkAndCreateLoadFolderNumber(baseStorePath, molapInfo.getTableName());

        fileManager = new LoadFolderData();
        fileManager.setName(loadFolderName + MolapCommonConstants.FILE_INPROGRESS_STATUS);

        dimensionWriter = new LevelValueWriter[dimInsertFileNames.length];
        for (int i = 0; i < dimensionWriter.length; i++) {
            String dimFileName =
                    dimInsertFileNames[i] + MolapCommonConstants.FILE_INPROGRESS_STATUS;
            dimensionWriter[i] = new LevelValueWriter(dimFileName, storeFolderWithLoadNumber);
            FileData fileData = new FileData(dimFileName, storeFolderWithLoadNumber);
            fileManager.add(fileData);
        }

        hierValueWriter = new HashMap<String, HierarchyValueWriter>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        for (Entry<String, String> entry : hierInsertFileNames.entrySet()) {
            String hierFileName =
                    entry.getValue().trim() + MolapCommonConstants.FILE_INPROGRESS_STATUS;
            hierValueWriter.put(entry.getKey(),
                    new HierarchyValueWriter(hierFileName, storeFolderWithLoadNumber));
            Map<String, KeyGenerator> keyGenerators = molapInfo.getKeyGenerators();
            keyGeneratorMap.put(entry.getKey(), keyGenerators.get(entry.getKey()));

            FileData fileData = new FileData(hierFileName, storeFolderWithLoadNumber);
            fileManager.add(fileData);
        }

        populateCache();

        this.currentRestructNumber = currentRestructNum;
    }

    private String checkAndCreateLoadFolderNumber(String baseStorePath, String tableName)
            throws KettleException {
        int restrctFolderCount = currentRestructNumber;
        //
        if (restrctFolderCount == -1) {
            restrctFolderCount = 0;
        }
        //
        String baseStorePathWithTableName =
                baseStorePath + File.separator + MolapCommonConstants.RESTRUCTRE_FOLDER
                        + restrctFolderCount + File.separator + tableName;
        int counter = MolapUtil.checkAndReturnCurrentLoadFolderNumber(baseStorePathWithTableName);
        counter++;
        String basePath =
                baseStorePathWithTableName + File.separator + MolapCommonConstants.LOAD_FOLDER
                        + counter;

        basePath = basePath + MolapCommonConstants.FILE_INPROGRESS_STATUS;
        loadFolderName = MolapCommonConstants.LOAD_FOLDER + counter;
        //
        boolean isDirCreated = new File(basePath).mkdirs();
        if (!isDirCreated) {
            throw new KettleException("Unable to create dataload directory" + basePath);
        }
        return basePath;
    }

    private void populateCache() throws KettleException {
        //
        checkAndUpdateFolderList(baseStorePath);
        // Fixing Check style
        String exceptionMsg = "";
        try {
            for (File folder : folderList) {
                exceptionMsg = "Not able to read level mapping File.";
                // update the member cache
                for (int i = 0; i < dimInsertFileNames.length; i++) {
                    File file = new File(
                            folder.getAbsolutePath() + File.separator + dimInsertFileNames[i]);

                    if (file.exists()) {
                        //
                        readLevelFileAndUpdateCache(file, dimInsertFileNames[i]);
                    }

                }

                // Update the hierarchy cache
                exceptionMsg = "Not able to read hierarchy mapping File.";
                for (Entry<String, String> entry : hierInsertFileNames.entrySet()) {
                    File hierarchyFile = new File(
                            folder.getAbsolutePath() + File.separator + entry.getKey()
                                    + HIERARCHY_FILE_EXTENSION);
                    //
                    if (hierarchyFile.exists()) {

                        readHierarchyAndUpdateCache(hierarchyFile, entry.getKey());

                    }

                }

            }
        } catch (IOException e) {
            throw new KettleException(exceptionMsg, e);
        }
        folderList.clear();

    }

    /**
     * This method recursively checks the folder with Load_ inside each and every RS_x/TableName/Load_x
     * and add in the folder list the load folders.
     *
     * @param baseStorePath
     * @return
     */
    private File[] checkAndUpdateFolderList(String baseStorePath) {
        File folders = new File(baseStorePath);
        //
        File[] rsFolders = folders.listFiles(new FileFilter() {
            @Override public boolean accept(File pathname) {
                if (pathname.isDirectory()
                        && pathname.getAbsolutePath().indexOf(MolapCommonConstants.LOAD_FOLDER)
                        > -1) {
                    return true;
                } else {
                    //
                    File[] checkFolder = checkAndUpdateFolderList(pathname.getAbsolutePath());
                    if (null != checkFolder) {
                        for (File f : checkFolder) {
                            folderList.add(f);
                        }
                    }
                }
                return false;
            }
        });

        return rsFolders;
    }

    @Override protected byte[] getHierFromStore(int[] val, String hier) throws KettleException

    {

        long[] value = new long[val.length];

        System.arraycopy(val, 0, value, 0, val.length);

        byte[] bytes;
        try {
            bytes = molapInfo.getKeyGenerators().get(hier).generateKey(value);
            hierValueWriter.get(hier).getByteArrayList().add(bytes);
        } catch (KeyGenException ex) {
            throw new KettleException(ex);
        }
        return bytes;

    }

    @Override protected int getSurrogateFromStore(String value, int index, Object[] properties)
            throws KettleException {
        max[index]++;
        int key = max[index];

        dimensionWriter[index].writeIntoLevelFile(value, key, properties);

        return key;
    }

    public void writeHeirDataToFileAndCloseStreams() throws KettleException {
        // For closing Level value writer bufferred streams
        for (int i = 0; i < dimensionWriter.length; i++) {
            String memberFileName = dimensionWriter[i].getMemberFileName();
            OutputStream bufferedOutputStream = dimensionWriter[i].getBufferedOutputStream();
            if (null == bufferedOutputStream) {
                continue;
            }
            MolapUtil.closeStreams(bufferedOutputStream);
            int size = fileManager.size();
            for (int j = 0; j < size; j++) {
                FileData fileData = (FileData) fileManager.get(j);
                String fileName = fileData.getFileName();
                if (memberFileName.equals(fileName)) {
                    String storePath = fileData.getStorePath();
                    String inProgFileName = fileData.getFileName();
                    String changedFileName = fileName.substring(0, fileName.lastIndexOf('.'));

                    File currentFile = new File(storePath + File.separator + inProgFileName);
                    File destFile = new File(storePath + File.separator + changedFileName);

                    if (!currentFile.renameTo(destFile)) {
                        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                                "Not Able to rename " + currentFile.getName() + " to " + destFile
                                        .getName());
                    }

                    break;
                }

            }

        }

        // For closing stream inside hierarchy writer 

        for (Entry<String, String> entry : hierInsertFileNames.entrySet()) {
            // First we need to sort the byte array
            List<byte[]> byteArrayList = hierValueWriter.get(entry.getKey()).getByteArrayList();
            String hierFileName = hierValueWriter.get(entry.getKey()).getHierarchyName();
            Collections.sort(byteArrayList, molapInfo.getKeyGenerators().get(entry.getKey()));
            byte[] bytesTowrite = null;
            for (byte[] bytes : byteArrayList) {
                bytesTowrite = new byte[bytes.length + 4];
                System.arraycopy(bytes, 0, bytesTowrite, 0, bytes.length);
                hierValueWriter.get(entry.getKey()).writeIntoHierarchyFile(bytesTowrite);
            }

            // now write the byte array in the file.
            BufferedOutputStream bufferedOutStream =
                    hierValueWriter.get(entry.getKey()).getBufferedOutStream();
            if (null == bufferedOutStream) {
                continue;
            }
            MolapUtil.closeStreams(hierValueWriter.get(entry.getKey()).getBufferedOutStream());

            int size = fileManager.size();

            for (int j = 0; j < size; j++) {
                FileData fileData = (FileData) fileManager.get(j);
                String fileName = fileData.getFileName();
                if (hierFileName.equals(fileName)) {
                    String storePath = fileData.getStorePath();
                    String inProgFileName = fileData.getFileName();
                    String changedFileName = fileName.substring(0, fileName.lastIndexOf('.'));
                    File currentFile = new File(storePath + File.separator + inProgFileName);
                    File destFile = new File(storePath + File.separator + changedFileName);

                    currentFile.renameTo(destFile);

                    fileData.setName(changedFileName);

                    break;
                }

            }
        }
    }

    private void readHierarchyAndUpdateCache(File hierarchyFile, String hierarchy)
            throws IOException {
        KeyGenerator generator = keyGeneratorMap.get(hierarchy);
        int keySizeInBytes = generator.getKeySizeInBytes();

        FileInputStream inputStream = null;
        FileChannel fileChannel = null;
        ByteBuffer byteBuffer = ByteBuffer.allocate(keySizeInBytes + 4);
        try {
            inputStream = new FileInputStream(hierarchyFile);
            fileChannel = inputStream.getChannel();

            while (fileChannel.read(byteBuffer) != -1) {
                byte[] array = byteBuffer.array();
                byte[] keyByteArray = new byte[keySizeInBytes];
                System.arraycopy(array, 0, keyByteArray, 0, keySizeInBytes);
                long[] keyArray = generator.getKeyArray(keyByteArray);
                int[] hirerarchyValues = new int[keyArray.length];
                // Change long to int
                for (int i = 0; i < keyArray.length; i++) {
                    //CHECKSTYLE:OFF
                    hirerarchyValues[i] = (int) keyArray[i];
                    //CHECKSTYLE:ON

                }
                // update the cache
                updateHierCache(hirerarchyValues, hierarchy);

                byteBuffer.clear();
            }
        } finally {
            MolapUtil.closeStreams(fileChannel, inputStream);
        }

    }

    private void updateHierCache(int[] hirerarchyValues, String hierarchy) {
        //
        IntArrayWrapper wrapper = new IntArrayWrapper(hirerarchyValues, 0);
        Map<IntArrayWrapper, Boolean> hCache = hierCache.get(hierarchy);
        //
        Boolean b = hCache.get(wrapper);
        if (b != null) {
            return;
        }
        //
        wLock2.lock();
        if (null == hCache.get(wrapper)) {
            // Store in cache
            hCache.put(wrapper, true);
        }
        wLock2.unlock();

    }

    private void readLevelFileAndUpdateCache(File memberFile, String dimInsertFileNames)
            throws IOException, KettleException {
        // create an object of FileOutputStream
        FileInputStream fos = new FileInputStream(memberFile);

        FileChannel fileChannel = fos.getChannel();
        try {
            Map<String, Integer> memberMap = memberCache.get(dimInsertFileNames);
            // ByteBuffer toltalLength, memberLength, surrogateKey, bf3;
            long size = fileChannel.size();
            int maxKey = 0;      //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_005
            boolean enableEncoding = Boolean.valueOf(MolapProperties.getInstance()
                    .getProperty(MolapCommonConstants.ENABLE_BASE64_ENCODING,
                            MolapCommonConstants.ENABLE_BASE64_ENCODING_DEFAULT));
            //CHECKSTYLE:ON
            while (fileChannel.position() < size) {
                ByteBuffer rowlengthToRead = ByteBuffer.allocate(4);
                fileChannel.read(rowlengthToRead);
                rowlengthToRead.rewind();
                int len = rowlengthToRead.getInt();

                ByteBuffer row = ByteBuffer.allocate(len);
                fileChannel.read(row);
                row.rewind();
                int toread = row.getInt();
                byte[] bytes = new byte[toread];
                row.get(bytes);
                String value = null;//CHECKSTYLE:OFF

                if (enableEncoding) {
                    value = new String(Base64.decodeBase64(bytes), Charset.defaultCharset());
                } else {
                    value = new String(bytes, Charset.defaultCharset());
                }

                int surrogateValue = row.getInt();
                memberMap.put(value, surrogateValue);

                // check if max key is less than Surrogate key then update the max
                // key
                maxKey = surrogateValue;
            }

            //Check the previous value assigned for surrogate key and initialize the value.
            checkAndUpdateMap(maxKey, dimInsertFileNames);
        } finally {
            MolapUtil.closeStreams(fileChannel, fos);
        }

    }

    private void checkAndUpdateMap(int maxKey, String dimInsertFileNames) {
        for (int i = 0; i < dimsFiles.length; i++) {
            if (dimInsertFileNames.equalsIgnoreCase(dimsFiles[i])) {
                max[i] = maxKey;
                break;
            }
        }

    }

    @Override protected byte[] getNormalizedHierFromStore(int[] val, String hier,
            HierarchyValueWriter hierWriter) throws KettleException {
        byte[] bytes;
        try {
            bytes = molapInfo.getKeyGenerators().get(hier).generateKey(val);
            hierWriter.getByteArrayList().add(bytes);
        } catch (KeyGenException e) {
            throw new KettleException(e);
        }
        return bytes;
    }

}

