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

package com.huawei.unibi.molap.globalsurrogategenerator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.*;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.filesystem.MolapFile;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory.FileType;
import com.huawei.unibi.molap.olap.MolapDef.*;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.util.MolapSchemaParser;
import com.huawei.unibi.molap.util.MolapUtil;
import org.apache.commons.codec.binary.Base64;

public class LevelGlobalSurrogateGeneratorThread implements Callable<Void> {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(LevelGlobalSurrogateGeneratorThread.class.getName());
    private String[][] partitionLocation;
    private CubeDimension dimension;
    private Schema schema;
    private String tableName;
    private String partitionColumn;

    public LevelGlobalSurrogateGeneratorThread(final String[][] partitionLocation,
            final CubeDimension dimension, final Schema schema, final String tableName,
            final String partitionColumn) {
        this.partitionLocation = partitionLocation;
        this.dimension = dimension;
        this.schema = schema;
        this.tableName = tableName;
        this.partitionColumn = partitionColumn;
    }

    public static Map<String, Integer> readLevelFileAndUpdateCache(MolapFile memberFile)
            throws IOException {
        DataInputStream inputStream = null;
        Map<String, Integer> localMemberMap =
                new HashMap<String, Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);

        try {
            inputStream = FileFactory.getDataInputStream(memberFile.getPath(),
                    FileFactory.getFileType(memberFile.getPath()));

            long currentPosition = 4;
            long size = memberFile.getSize() - 4;

            boolean enableEncoding = Boolean.valueOf(MolapProperties.getInstance()
                    .getProperty(MolapCommonConstants.ENABLE_BASE64_ENCODING,
                            MolapCommonConstants.ENABLE_BASE64_ENCODING_DEFAULT));
            int surrogateValue = inputStream.readInt();
            while (currentPosition < size) {
                int len = inputStream.readInt();
                currentPosition += 4;
                byte[] rowBytes = new byte[len];

                inputStream.readFully(rowBytes);
                currentPosition += len;
                String decodedValue = null;

                if (enableEncoding) {
                    decodedValue =
                            new String(Base64.decodeBase64(rowBytes), Charset.defaultCharset());
                } else {
                    decodedValue = new String(rowBytes, Charset.defaultCharset());
                }
                localMemberMap.put(decodedValue, surrogateValue);
                surrogateValue++;
            }

        } catch (Exception e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    e.getMessage());
            MolapUtil.closeStreams(inputStream);
        } finally {
            MolapUtil.closeStreams(inputStream);
        }
        return localMemberMap;
    }

    @Override public Void call() throws Exception {

        long currentTimeMillis = System.currentTimeMillis();
        long currentTimeMillis1 = System.currentTimeMillis();

        Hierarchy[] extractHierarchies = MolapSchemaParser.extractHierarchies(schema, dimension);
        Level cubeLevel = extractHierarchies[0].levels[0];
        boolean isPartitionColumn = partitionColumn.equals(cubeLevel.name);
        if (partitionColumn.equals(cubeLevel.name)) {
            isPartitionColumn = true;
        }

        RelationOrJoin relation = extractHierarchies[0].relation;
        String hierarchyTable =
                relation == null ? tableName : ((Table) extractHierarchies[0].relation).name;
        String levelFileName = hierarchyTable + '_' + cubeLevel.name;

        List<PartitionMemberVo> partitionMemberVoList =
                new ArrayList<PartitionMemberVo>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        ExecutorService ex = Executors.newFixedThreadPool(10);

        PartitionMemberVo memberVo = null;

        List<Future<Map<String, Integer>>> submitList =
                new ArrayList<Future<Map<String, Integer>>>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        for (int i = 0; i < partitionLocation.length; i++) {

            int partitionLength = partitionLocation[i].length;
            if (partitionLength == 0) {
                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "partition length is 0");
                continue;
            }
            String path =
                    partitionLocation[i][partitionLength - 1] + '/' + levelFileName + ".level";

            FileType fileType = FileFactory.getFileType(path);
            if (!FileFactory.isFileExist(path, fileType)) {
                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "File does not exist at path :: " + path);
                continue;
            }
            MolapFile molapFile = FileFactory.getMolapFile(path, fileType);

            memberVo = new PartitionMemberVo();
            memberVo.setPath(partitionLocation[i][partitionLength - 1]);
            partitionMemberVoList.add(memberVo);
            Future<Map<String, Integer>> submit = ex.submit(new ReaderThread(molapFile));
            submitList.add(submit);
        }

        ex.shutdown();
        ex.awaitTermination(1, TimeUnit.DAYS);
        if (partitionMemberVoList.size() < 1) {
            return null;
        }
        int maxSeqenceKey = getMaxSequenceKeyAssigned(levelFileName + ".globallevel");
        int index = 0;
        for (Future<Map<String, Integer>> future : submitList) {
            partitionMemberVoList.get(index).setMembersMap(future.get());
            index++;
        }

        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Time Taken to read surrogate for Level: " + levelFileName + " : " + (
                        System.currentTimeMillis() - currentTimeMillis));

        currentTimeMillis = System.currentTimeMillis();

        ex = Executors.newFixedThreadPool(5);

        createGlobalSurrogateKey(currentTimeMillis, currentTimeMillis1, isPartitionColumn,
                levelFileName, partitionMemberVoList, ex, maxSeqenceKey);

        return null;
    }

    private void createGlobalSurrogateKey(long currentTimeMillis, long currentTimeMillis1,
            boolean isPartitionColumn, String levelFileName,
            List<PartitionMemberVo> partitionMemberVoList, ExecutorService ex, int maxSeqenceKey)
            throws InterruptedException {
        int[] key = new int[partitionMemberVoList.get(0).getMembersMap().size()];
        int[] value = new int[partitionMemberVoList.get(0).getMembersMap().size()];

        int[] localKey = null;
        int[] localValue = null;
        int countVal = 0;
        int minSeqenceKey = Integer.MAX_VALUE;
        if (!isPartitionColumn) {
            for (Entry<String, Integer> entryInfo : partitionMemberVoList.get(0).getMembersMap()
                    .entrySet()) {
                if (minSeqenceKey > entryInfo.getValue()) {
                    minSeqenceKey = entryInfo.getValue();
                }
                key[countVal] = entryInfo.getValue();
                value[countVal] = ++maxSeqenceKey;
                countVal++;
            }

            localKey = key;
            localValue = value;
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Time Taken to generate global surrogate for Level: " + levelFileName + " : "
                            + (System.currentTimeMillis() - currentTimeMillis));

            currentTimeMillis = System.currentTimeMillis();

            ex.submit(new WriterThread(localKey, localValue, partitionMemberVoList.get(0).getPath(),
                    levelFileName + ".globallevel", maxSeqenceKey, minSeqenceKey));

            processNonPartitionedColumn(currentTimeMillis, levelFileName, partitionMemberVoList, ex,
                    maxSeqenceKey);
        } else {
            for (int i = 0; i < partitionMemberVoList.size(); i++) {
                countVal = 0;
                minSeqenceKey = Integer.MAX_VALUE;
                key = new int[partitionMemberVoList.get(i).getMembersMap().size()];
                value = new int[partitionMemberVoList.get(i).getMembersMap().size()];
                for (Entry<String, Integer> entry : partitionMemberVoList.get(i).getMembersMap()
                        .entrySet()) {
                    if (minSeqenceKey > entry.getValue()) {
                        minSeqenceKey = entry.getValue();
                    }
                    key[countVal] = entry.getValue();
                    value[countVal] = ++maxSeqenceKey;
                    countVal++;
                }

                localKey = key;
                localValue = value;
                LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Time Taken to generate global surrogate for Level: " + levelFileName
                                + " : " + (System.currentTimeMillis() - currentTimeMillis));
                currentTimeMillis = System.currentTimeMillis();
                ex.submit(new WriterThread(localKey, localValue,
                        partitionMemberVoList.get(i).getPath(), levelFileName + ".globallevel",
                        maxSeqenceKey, minSeqenceKey));
            }
        }

        LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                "Time Taken to write global surrogate for Level: " + levelFileName + " : " + (
                        System.currentTimeMillis() - currentTimeMillis1));

        ex.shutdown();
        ex.awaitTermination(1, TimeUnit.DAYS);
    }

    private void processNonPartitionedColumn(long currentTimeMillis, String levelFileName,
            List<PartitionMemberVo> partitionMemberVoList, ExecutorService ex, int maxSeqenceKey) {
        int[] key;
        int[] value;
        int[] localKey;
        int[] localValue;
        int counter;
        int minSeqenceKey;
        Integer surrogateKey = null;
        boolean isFound = false;
        List<Integer> notFoundSurrogateKeys = null;
        for (int i = 1; i < partitionMemberVoList.size(); i++) {
            counter = 0;
            minSeqenceKey = Integer.MAX_VALUE;
            key = new int[partitionMemberVoList.get(i).getMembersMap().size()];
            value = new int[partitionMemberVoList.get(i).getMembersMap().size()];

            notFoundSurrogateKeys = new ArrayList<Integer>();
            for (Entry<String, Integer> entry : partitionMemberVoList.get(i).getMembersMap()
                    .entrySet()) {
                surrogateKey = null;
                isFound = false;
                for (int j = 0; j < i; j++) {
                    surrogateKey = partitionMemberVoList.get(j).getMembersMap().get(entry.getKey());
                    if (null != surrogateKey) {
                        isFound = true;
                        if (minSeqenceKey > entry.getValue()) {
                            minSeqenceKey = entry.getValue();
                        }
                        key[counter] = entry.getValue();
                        value[counter] = surrogateKey;
                        counter++;
                        break;
                    }
                }
                if (!isFound) {
                    notFoundSurrogateKeys.add(entry.getValue());
                }
            }
            for (Integer notFoundSurrgates : notFoundSurrogateKeys) {
                key[counter] = notFoundSurrgates;
                value[counter] = ++maxSeqenceKey;
                counter++;
            }
            localKey = key;
            localValue = value;
            ex.submit(new WriterThread(localKey, localValue, partitionMemberVoList.get(i).getPath(),
                    levelFileName + ".globallevel", maxSeqenceKey, minSeqenceKey));
            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                    "Time Taken to generate global surrogate for Level: " + levelFileName + " : "
                            + (System.currentTimeMillis() - currentTimeMillis));

            currentTimeMillis = System.currentTimeMillis();
        }
    }

    private int getMaxSequenceKeyAssigned(String levelFileName) {
        int maxKey = 0;
        if (partitionLocation[0].length < 2) {
            return maxKey;
        }
        int maxKeyAssigned = 0;
        for (int i = 0; i < partitionLocation.length; i++) {
            for (int j = 0; j < partitionLocation[i].length - 1; j++) {
                MolapFile molapFile = FileFactory
                        .getMolapFile(partitionLocation[i][j] + '/' + levelFileName, FileFactory
                                        .getFileType(
                                                partitionLocation[i][j] + '/' + levelFileName));
                if (molapFile.exists()) {
                    maxKeyAssigned = getMaxKeyAssigned(molapFile);
                    if (maxKey < maxKeyAssigned) {
                        maxKey = maxKeyAssigned;
                    }
                }
            }
        }
        return maxKey;
    }

    private int getMaxKeyAssigned(MolapFile memberFile) {
        DataInputStream inputStream = null;
        try {
            inputStream = FileFactory.getDataInputStream(memberFile.getPath(),
                    FileFactory.getFileType(memberFile.getPath()));
            return inputStream.readInt();

        } catch (FileNotFoundException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    e.getMessage());
        } catch (IOException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    e.getMessage());
        } finally {
            MolapUtil.closeStreams(inputStream);
        }
        return -1;

    }

    private void writeGlobalSurrogateKeyFile(String string, int[] key, int[] value, String fileName,
            int currentMaxKey, int minValue) {
        DataOutputStream stream = null;
        try {
            stream = FileFactory.getDataOutputStream(string + '/' + fileName,
                    FileFactory.getFileType(string + '/' + fileName), 10240);
            int size = key.length;
            stream.writeInt(currentMaxKey);
            stream.writeInt(minValue);
            stream.writeInt(size);
            for (int i = 0; i < size; i++) {
                stream.writeInt(key[i]);
                stream.writeInt(value[i]);
            }
        } catch (FileNotFoundException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    e.getMessage());
        } catch (IOException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    e.getMessage());
        } finally {
            MolapUtil.closeStreams(stream);
        }
    }

    private final class WriterThread implements Callable<Void> {
        int currentMaxKey;
        int minValue;
        private int[] key;
        private int[] value;
        private String path;
        private String fileName;

        private WriterThread(int[] key, int[] value, String path, String fileName,
                int currentMaxKey, int minValue) {
            this.key = key;
            this.value = value;
            this.path = path;
            this.fileName = fileName;
            this.currentMaxKey = currentMaxKey;
            this.minValue = minValue;
        }

        @Override public Void call() throws Exception {
            writeGlobalSurrogateKeyFile(path, key, value, fileName, currentMaxKey, minValue);
            return null;
        }

    }

}
