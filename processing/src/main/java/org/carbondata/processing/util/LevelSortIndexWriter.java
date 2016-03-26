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

package org.carbondata.processing.util;

import java.io.File;
import java.io.FileFilter;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.util.MolapProperties;

public class LevelSortIndexWriter {
    /**
     * Comment for <code>LOGGER</code>
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(LevelSortIndexWriter.class.getName());
    private ExecutorService executor;

    private Map<String, String> levelFileMap;

    /**
     * LevelSortIndexWriter
     *
     * @param levelFileMap
     */
    public LevelSortIndexWriter(Map<String, String> levelFileMap) {
        int parseInt = Integer.parseInt(MolapProperties.getInstance()
                .getProperty(MolapCommonConstants.NUM_CORES_LOADING, "2"));
        executor = Executors.newFixedThreadPool(parseInt);
        this.levelFileMap = levelFileMap;
    }

    /**
     * Below method is responsible for creating the level sort index file, it will submit the task to thread pool
     *
     * @param storeFilePath
     */
    public void updateLevelFiles(String storeFilePath) {
        File file = new File(storeFilePath);
        File[] levelFilesPresent = file.listFiles(new FileFilter() {
            @Override public boolean accept(File pathname) {
                return pathname.getName().endsWith(MolapCommonConstants.LEVEL_FILE_EXTENSION);
            }
        });
        if (null != levelFilesPresent && levelFilesPresent.length > 0) {
            for (int i = 0; i < levelFilesPresent.length; i++) {
                executor.submit(
                        new LevelSortIndexWriterThread(levelFilesPresent[i].getAbsolutePath(),
                                levelFileMap.get(levelFilesPresent[i].getName())));
            }
        }
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e,
                    e.getMessage());
        }

    }
}
