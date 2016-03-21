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

package com.huawei.unibi.molap.datastorage.store.filesystem;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.util.MolapCoreLogEvent;

public class LocalMolapFile implements MolapFile {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(LocalMolapFile.class.getName());
    private File file;

    public LocalMolapFile(String path) {
        file = new File(path);
    }

    public LocalMolapFile(File file) {
        this.file = file;
    }

    @Override public String getAbsolutePath() {
        return file.getAbsolutePath();
    }

    @Override public MolapFile[] listFiles(final MolapFileFilter fileFilter) {
        if (!file.isDirectory()) {
            return null;
        }

        File[] files = file.listFiles(new FileFilter() {

            @Override public boolean accept(File pathname) {
                return fileFilter.accept(new LocalMolapFile(pathname));
            }
        });

        if (files == null) {
            return new MolapFile[0];
        }

        MolapFile[] molapFiles = new MolapFile[files.length];

        for (int i = 0; i < molapFiles.length; i++) {
            molapFiles[i] = new LocalMolapFile(files[i]);
        }

        return molapFiles;
    }

    @Override public String getName() {
        return file.getName();
    }

    @Override public boolean isDirectory() {
        return file.isDirectory();
    }

    @Override public boolean exists() {
        return file.exists();
    }

    @Override public String getCanonicalPath() {
        try {
            return file.getCanonicalPath();
        } catch (IOException e) {
            LOGGER.error(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, e,
                    "Exception occured" + e.getMessage());
        }
        return null;
    }

    @Override public MolapFile getParentFile() {
        return new LocalMolapFile(file.getParentFile());
    }

    @Override public String getPath() {
        return file.getPath();
    }

    @Override public long getSize() {
        return file.length();
    }

    public boolean renameTo(String changetoName) {
        return file.renameTo(new File(changetoName));
    }

    public boolean delete() {
        return file.delete();
    }

    @Override public MolapFile[] listFiles() {

        if (!file.isDirectory()) {
            return null;
        }
        File[] files = file.listFiles();
        if (files == null) {
            return new MolapFile[0];
        }
        MolapFile[] molapFiles = new MolapFile[files.length];
        for (int i = 0; i < molapFiles.length; i++) {
            molapFiles[i] = new LocalMolapFile(files[i]);
        }

        return molapFiles;

    }

    @Override public boolean createNewFile() {
        try {
            return file.createNewFile();
        } catch (IOException e) {
            return false;
        }
    }

    @Override public boolean mkdirs() {
        return file.mkdirs();
    }

    @Override public long getLastModifiedTime() {
        return file.lastModified();
    }

    @Override public boolean setLastModifiedTime(long timestamp) {
        return file.setLastModified(timestamp);
    }

    @Override public boolean renameForce(String changetoName) {
        File destFile = new File(changetoName);
        if (destFile.exists()) {
            if (destFile.delete()) {
                return file.renameTo(new File(changetoName));
            }
        }

        return file.renameTo(new File(changetoName));

    }
}
