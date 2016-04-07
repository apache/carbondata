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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.util.CarbonCoreLogEvent;

/**
 * @author Administrator
 */
public class LocalFileLock extends AbstractCarbonLock {
    private String location;

    private LockType lockType;

    private FileOutputStream fileOutputStream;

    private FileChannel channel;

    private FileLock fileLock;

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(LocalFileLock.class.getName());

    /**
     * @param location
     */
    public LocalFileLock(String location, LockType lockType) {
        this.lockType = lockType;
        this.location = location;
        if (this.lockType == LockType.METADATA_LOCK) {
            this.location = location + File.separator + CarbonCommonConstants.METADATA_LOCK;
        }
        initRetry();
    }

    /* (non-Javadoc)
     * @see org.carbondata.core.locks.ICarbonLock#lock()
     */
    @Override public boolean lock() {
        try {
            if (!FileFactory.isFileExist(location, FileFactory.getFileType(location))) {
                FileFactory.createNewLockFile(location, FileFactory.getFileType(location));
            }

            fileOutputStream = new FileOutputStream(location);
            channel = fileOutputStream.getChannel();
            try {
                fileLock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                return false;
            }
            if (null != fileLock) {
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            return false;
        }

    }

    /* (non-Javadoc)
     * @see org.carbondata.core.locks.ICarbonLock#unlock()
     */
    @Override public boolean unlock() {
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

}
