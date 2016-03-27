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

package org.carbondata.core.locks;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.util.CarbonCoreLogEvent;
import org.carbondata.core.util.CarbonProperties;

/**
 * Class Provides generic implementation of the lock in Carbon.
 */
public abstract class CarbonLock {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonLock.class.getName());
    protected String location;
    private boolean isLocked;
    private DataOutputStream dataOutputStream;

    private boolean isLocal;

    private FileChannel channel;

    private FileOutputStream fileOutputStream;

    private FileLock fileLock;

    private int retryCount;

    private int retryTimeout;

    protected void initRetry() {
        String retries = CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK);
        try {
            retryCount = Integer.parseInt(retries);
        } catch (NumberFormatException e) {
            retryCount = CarbonCommonConstants.NUMBER_OF_TRIES_FOR_LOAD_METADATA_LOCK_DEFAULT;
        }

        String maxTimeout = CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK);
        try {
            retryTimeout = Integer.parseInt(maxTimeout);
        } catch (NumberFormatException e) {
            retryTimeout = CarbonCommonConstants.MAX_TIMEOUT_FOR_LOAD_METADATA_LOCK_DEFAULT;
        }

    }

    /**
     * This API will provide file based locking mechanism
     * In HDFS locking is handled using the hdfs append API which provide only one stream at a time.
     * In local file system locking is handled using the file channel.
     */
    private boolean lock() {

        if (FileFactory.getFileType(location) == FileType.LOCAL) {
            isLocal = true;
        }
        try {
            if (!FileFactory.isFileExist(location, FileFactory.getFileType(location))) {
                FileFactory.createNewLockFile(location, FileFactory.getFileType(location));
            }
        } catch (IOException e) {
            isLocked = false;
            return isLocked;
        }

        if (isLocal) {
            localFileLocking();
        } else {
            hdfsFileLocking();
        }
        return isLocked;
    }

    /**
     * Handling of the locking in HDFS file system.
     */
    private void hdfsFileLocking() {
        try {

            dataOutputStream = FileFactory
                    .getDataOutputStreamUsingAppend(location, FileFactory.getFileType(location));

            isLocked = true;

        } catch (IOException e) {
            isLocked = false;
        }
    }

    /**
     * Handling of the locking in local file system using file channel.
     */
    private void localFileLocking() {
        try {

            fileOutputStream = new FileOutputStream(location);
            channel = fileOutputStream.getChannel();

            fileLock = channel.tryLock();
            if (null != fileLock) {
                isLocked = true;
            } else {
                isLocked = false;
            }

        } catch (IOException e) {
            isLocked = false;
        }
    }

    /**
     * This API will release the stream of locked file, Once operation in the metadata has been done
     *
     * @return
     */
    public boolean unlock() {
        boolean status = false;

        if (isLock()) {

            if (isLocal) {
                status = localLockFileUnlock();
            } else {
                status = hdfslockFileUnlock(status);

            }
        }
        return status;
    }

    /**
     * handling of the hdfs file unlocking.
     *
     * @param status
     * @return
     */
    private boolean hdfslockFileUnlock(boolean status) {
        if (null != dataOutputStream) {
            try {
                dataOutputStream.close();
                status = true;
            } catch (IOException e) {
                status = false;
            }
        }
        return status;
    }

    /**
     * handling of the local file unlocking.
     *
     * @return
     */
    private boolean localLockFileUnlock() {
        boolean status;
        try {
            fileLock.release();
            status = true;
        } catch (IOException e) {
            status = false;
        } finally {
            if (null != fileOutputStream) {
                try {
                    fileOutputStream.close();
                } catch (IOException e) {
                    LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, e.getMessage());
                }
            }
        }
        return status;
    }

    /**
     * To check whether the file is locked or not.
     */
    public boolean isLock() {
        return isLocked;
    }

    /**
     * API for enabling the locking of file.
     */
    public boolean lockWithRetries() {
        try {
            for (int i = 0; i < retryCount; i++) {
                if (lock()) {
                    return true;
                } else {
                    Thread.sleep(retryTimeout * 1000L);
                }
            }
        } catch (InterruptedException e) {
            return false;
        }
        return false;
    }

}
