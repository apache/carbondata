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

package org.carbondata.core.datastorage.store.impl;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.HDFSCarbonFile;
import org.carbondata.core.datastorage.store.filesystem.LocalCarbonFile;
import org.carbondata.core.util.CarbonUtil;

public final class FileFactory {
    private static Configuration configuration = null;

    private static FileType storeDefaultFileType = FileType.LOCAL;

    static {
        String property = CarbonUtil.getCarbonStorePath(null, null);
        if (property != null) {
            if (property.startsWith("hdfs://")) {
                storeDefaultFileType = FileType.HDFS;
            }
        }

        configuration = new Configuration();
        configuration.addResource(new Path("../core-default.xml"));
    }

    private FileFactory() {

    }

    public static Configuration getConfiguration() {
        return configuration;
    }

    public static FileHolder getFileHolder(FileType fileType) {
        switch (fileType) {
            case LOCAL:
                return new FileHolderImpl();
            case HDFS:
                return new HDFSFileHolderImpl();
            default:
                return new FileHolderImpl();
        }
    }

    public static FileType getFileType() {
        String property = CarbonUtil.getCarbonStorePath(null, null);
        if (property != null) {
            if (property.startsWith("hdfs://")) {
                storeDefaultFileType = FileType.HDFS;
            }
        }
        return storeDefaultFileType;
    }

    public static FileType getFileType(String path) {
        if (path.startsWith("hdfs://")) {
            return FileType.HDFS;
        }
        return FileType.LOCAL;
    }

    public static CarbonFile getCarbonFile(String path, FileType fileType) {
        switch (fileType) {
            case LOCAL:
                return new LocalCarbonFile(path);
            case HDFS:
                return new HDFSCarbonFile(path);
            default:
                return new LocalCarbonFile(path);
        }
    }

    public static DataInputStream getDataInputStream(String path, FileType fileType)
            throws IOException {
        path = path.replace("\\", "/");
        switch (fileType) {
            case LOCAL:
                return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
            case HDFS:
                Path pt = new Path(path);
                FileSystem fs = pt.getFileSystem(configuration);
                FSDataInputStream stream = fs.open(pt);
                return stream;
            default:
                return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
        }
    }

    public static DataInputStream getDataInputStream(String path, FileType fileType, int bufferSize)
            throws IOException {
        path = path.replace("\\", "/");
        switch (fileType) {
            case LOCAL:
                return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
            case HDFS:
                Path pt = new Path(path);
                FileSystem fs = pt.getFileSystem(configuration);
                FSDataInputStream stream = fs.open(pt, bufferSize);
                return stream;
            default:
                return new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
        }
    }

    public static DataOutputStream getDataOutputStream(String path, FileType fileType)
            throws IOException {
        path = path.replace("\\", "/");
        switch (fileType) {
            case LOCAL:
                return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
            case HDFS:
                Path pt = new Path(path);
                FileSystem fs = pt.getFileSystem(configuration);
                FSDataOutputStream stream = fs.create(pt, true);
                return stream;
            default:
                return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
        }
    }

    public static DataOutputStream getDataOutputStream(String path, FileType fileType,
            short replicationFactor) throws IOException {
        path = path.replace("\\", "/");
        switch (fileType) {
            case LOCAL:
                return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
            case HDFS:
                Path pt = new Path(path);
                FileSystem fs = pt.getFileSystem(configuration);
                FSDataOutputStream stream = fs.create(pt, replicationFactor);
                return stream;
            default:
                return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
        }
    }

    public static DataOutputStream getDataOutputStream(String path, FileType fileType,
            int bufferSize) throws IOException {
        path = path.replace("\\", "/");
        switch (fileType) {
            case LOCAL:
                return new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(path), bufferSize));
            case HDFS:
                Path pt = new Path(path);
                FileSystem fs = pt.getFileSystem(configuration);
                FSDataOutputStream stream = fs.create(pt, true, bufferSize);
                return stream;
            default:
                return new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(path), bufferSize));
        }
    }

    public static DataOutputStream getDataOutputStream(String path, FileType fileType,
            int bufferSize, boolean append) throws IOException {
        path = path.replace("\\", "/");
        switch (fileType) {
            case LOCAL:
                return new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(path, append), bufferSize));
            case HDFS:
                Path pt = new Path(path);
                FileSystem fs = pt.getFileSystem(configuration);
                FSDataOutputStream stream = fs.create(pt, append, bufferSize);
                return stream;
            default:
                return new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(path), bufferSize));
        }
    }

    public static List<String> getFileNames(final String extn, String folderPath, FileType fileType)
            throws IOException {
        folderPath = folderPath.replace("\\", "/");
        List<String> fileNames = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        switch (fileType) {
            case HDFS:
                Path pt = new Path(folderPath);
                FileSystem fs = pt.getFileSystem(configuration);
                FileStatus[] hdfsFiles = fs.listStatus(pt);
                for (FileStatus fileStatus : hdfsFiles) {
                    if (extn != null) {
                        if (!fileStatus.isDir() && fileStatus.getPath().getName().endsWith(extn)) {
                            fileNames.add(fileStatus.getPath().getName().replace(extn, ""));
                        }
                    } else {
                        if (!fileStatus.isDir()) {
                            fileNames.add(fileStatus.getPath().getName());
                        }
                    }

                }
                break;
            case LOCAL:
            default:
                File[] files = new File(folderPath).listFiles(new FileFilter() {
                    @Override public boolean accept(File pathname) {
                        if (pathname.isDirectory()) {
                            return false;
                        }

                        if (extn != null) {
                            return pathname.getName().endsWith(extn);
                        }
                        return true;
                    }
                });

                for (File oneFile : files) {
                    if (extn != null) {
                        fileNames.add(oneFile.getName().replace(extn, ""));
                    } else {
                        fileNames.add(oneFile.getName());
                    }
                }
        }

        return fileNames;
    }

    /**
     * This method checks the given path exists or not and also is it file or
     * not if the performFileCheck is true
     *
     * @param filePath         - Path
     * @param fileType         - FileType Local/HDFS
     * @param performFileCheck - Provide false for folders, true for files and
     */
    public static boolean isFileExist(String filePath, FileType fileType, boolean performFileCheck)
            throws IOException {
        filePath = filePath.replace("\\", "/");
        switch (fileType) {
            case HDFS:
                Path path = new Path(filePath);
                FileSystem fs = path.getFileSystem(configuration);
                if (performFileCheck) {
                    return fs.exists(path) && fs.isFile(path);
                } else {
                    return fs.exists(path);
                }

            case LOCAL:
            default:
                File defaultFile = new File(filePath);

                if (performFileCheck) {
                    return defaultFile.exists() && defaultFile.isFile();
                } else {
                    return defaultFile.exists();
                }
        }
    }

    /**
     * This method checks the given path exists or not and also is it file or
     * not if the performFileCheck is true
     *
     * @param filePath - Path
     * @param fileType - FileType Local/HDFS
     */
    public static boolean isFileExist(String filePath, FileType fileType) throws IOException {
        filePath = filePath.replace("\\", "/");
        switch (fileType) {
            case HDFS:
                Path path = new Path(filePath);
                FileSystem fs = path.getFileSystem(configuration);
                return fs.exists(path);

            case LOCAL:
            default:
                File defaultFile = new File(filePath);
                return defaultFile.exists();
        }
    }

    public static boolean createNewFile(String filePath, FileType fileType) throws IOException {
        filePath = filePath.replace("\\", "/");
        switch (fileType) {
            case HDFS:
                Path path = new Path(filePath);
                FileSystem fs = path.getFileSystem(configuration);
                return fs.createNewFile(path);

            case LOCAL:
            default:
                File file = new File(filePath);
                return file.createNewFile();
        }
    }

    public static boolean mkdirs(String filePath, FileType fileType) throws IOException {
        filePath = filePath.replace("\\", "/");
        switch (fileType) {
            case HDFS:
                Path path = new Path(filePath);
                FileSystem fs = path.getFileSystem(configuration);
                return fs.mkdirs(path);
            case LOCAL:
            default:
                File file = new File(filePath);
                return file.mkdirs();
        }
    }

    /**
     * for getting the dataoutput stream using the hdfs filesystem append API.
     *
     * @param path
     * @param fileType
     * @return
     * @throws IOException
     */
    public static DataOutputStream getDataOutputStreamUsingAppend(String path, FileType fileType)
            throws IOException {
        path = path.replace("\\", "/");
        switch (fileType) {
            case LOCAL:
                return new DataOutputStream(
                        new BufferedOutputStream(new FileOutputStream(path, true)));
            case HDFS:
                Path pt = new Path(path);
                FileSystem fs = pt.getFileSystem(configuration);
                FSDataOutputStream stream = fs.append(pt);
                return stream;
            default:
                return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path)));
        }
    }

    /**
     * for creating a new Lock file and if it is successfully created
     * then in case of abrupt shutdown then the stream to that file will be closed.
     *
     * @param filePath
     * @param fileType
     * @return
     * @throws IOException
     */
    public static boolean createNewLockFile(String filePath, FileType fileType) throws IOException {
        filePath = filePath.replace("\\", "/");
        switch (fileType) {
            case HDFS:
                Path path = new Path(filePath);
                FileSystem fs = path.getFileSystem(configuration);
                if (fs.createNewFile(path)) {
                    fs.deleteOnExit(path);
                    return true;
                }
                return false;
            case LOCAL:
            default:
                File file = new File(filePath);
                return file.createNewFile();
        }
    }

    public enum FileType {
        LOCAL, HDFS
    }

}
