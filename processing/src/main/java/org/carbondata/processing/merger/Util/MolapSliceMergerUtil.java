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

package org.carbondata.processing.merger.Util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.MolapFile;
import org.carbondata.core.datastorage.store.filesystem.MolapFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.core.writer.ByteArrayHolder;
import org.carbondata.processing.merger.exeception.SliceMergerException;
import org.carbondata.processing.util.MolapDataProcessorLogEvent;

public final class MolapSliceMergerUtil {
    /**
     * Comment for <code>LOGGER</code>
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(MolapSliceMergerUtil.class.getName());

    private MolapSliceMergerUtil() {

    }

    /**
     * Below method will be used to get the file map
     * Map will contain Key as a type of file(File Name) and its list of files
     *
     * @param sliceFiles slice files
     * @return file map
     */
    public static Map<String, List<MolapFile>> getFileMap(MolapFile[][] sliceFiles) {
        Map<String, List<MolapFile>> filesMap = new LinkedHashMap<String, List<MolapFile>>();
        for (int i = 0; i < sliceFiles.length; i++) {
            for (int j = 0; j < sliceFiles[i].length; j++) {
                String fileName = sliceFiles[i][j].getName();
                List<MolapFile> fileList = filesMap.get(fileName);
                if (null == fileList) {
                    fileList = new ArrayList<MolapFile>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                    fileList.add(sliceFiles[i][j]);
                } else {
                    fileList.add(sliceFiles[i][j]);
                }
                filesMap.put(fileName, fileList);
            }
        }
        return filesMap;
    }

    /**
     * This method will be used for copy file from source to destination
     * location
     *
     * @param sourceLocation      source path
     * @param desTinationLocation destination path
     * @throws IOException if any problem while  reading or writing the files
     */
    public static void copyFile(MolapFile sourceLocation, String desTinationLocation)
            throws IOException {

        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            inputStream = FileFactory.getDataInputStream(sourceLocation.getAbsolutePath(),
                    FileFactory.getFileType(sourceLocation.getAbsolutePath()));
            outputStream = FileFactory.getDataOutputStream(desTinationLocation,
                    FileFactory.getFileType(desTinationLocation), 10240, true);

            copyFile(inputStream, outputStream);
        } finally {
            MolapUtil.closeStreams(inputStream, outputStream);
        }
    }

    /**
     * This metod copy the multiple level files and merge into single file.
     *
     * @param filesToMerge
     * @param destFile
     * @throws IOException
     */
    public static void copyMultipleFileToSingleFile(List<File> filesToMerge, File destFile)
            throws IOException {

        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            outputStream = new BufferedOutputStream(new FileOutputStream(destFile, true));

            for (File toMerge : filesToMerge) {
                inputStream = new BufferedInputStream(new FileInputStream(toMerge));
                copyFileWithoutClosingOutputStream(inputStream, outputStream);
            }
        } finally {
            MolapUtil.closeStreams(inputStream, outputStream);
        }

    }

    /**
     * This method reads the hierarchy file, sort the Mdkey and write into the destination
     * file.
     *
     * @param filesToMerge
     * @param destFile
     * @throws IOException
     */
    public static void mergeHierarchyFiles(List<File> filesToMerge, File destFile,
            int keySizeInBytes) throws IOException {
        List<ByteArrayHolder> holder =
                new ArrayList<ByteArrayHolder>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        for (File hierFiles : filesToMerge) {
            readHierarchyFile(hierFiles, keySizeInBytes, holder);
        }

        Collections.sort(holder);

        FileOutputStream fos = null;
        FileChannel outPutFileChannel = null;

        try {

            boolean isFileCreated = false;
            if (!destFile.exists()) {
                isFileCreated = destFile.createNewFile();

                if (!isFileCreated) {
                    throw new IOException("unable to create file" + destFile.getAbsolutePath());
                }
            }

            fos = new FileOutputStream(destFile);

            outPutFileChannel = fos.getChannel();
            for (ByteArrayHolder arrayHolder : holder) {
                try {
                    writeIntoHierarchyFile(arrayHolder.getMdKey(), arrayHolder.getPrimaryKey(),
                            outPutFileChannel);
                } catch (SliceMergerException e) {
                    LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                            "Unable to write hierarchy file");
                    throw new IOException(e);
                }

            }

        } finally {
            MolapUtil.closeStreams(outPutFileChannel, fos);
        }

    }

    private static void writeIntoHierarchyFile(byte[] bytes, int primaryKey,
            FileChannel outPutFileChannel) throws SliceMergerException {

        ByteBuffer byteBuffer = storeValueInCache(bytes, primaryKey);

        try {
            byteBuffer.flip();
            outPutFileChannel.write(byteBuffer);
        } catch (IOException e) {
            throw new SliceMergerException(
                    "Error while writting in the hierarchy mapping file at the merge step", e);
        }
    }

    private static ByteBuffer storeValueInCache(byte[] bytes, int primaryKey) {

        // adding 4 to store the total length of the row at the beginning
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 4);

        buffer.put(bytes);
        buffer.putInt(primaryKey);

        return buffer;
    }

    /**
     * setHeirAndKeySizeMap
     *
     * @param heirAndKeySize void
     */
    public static Map<String, Integer> getHeirAndKeySizeMap(String heirAndKeySize) {
        String[] split = heirAndKeySize.split(MolapCommonConstants.AMPERSAND_SPC_CHARACTER);
        String[] split2 = null;
        Map<String, Integer> heirAndKeySizeMap = new HashMap<String, Integer>(split.length);
        for (int i = 0; i < split.length; i++) {
            split2 = split[i].split(MolapCommonConstants.COLON_SPC_CHARACTER);
            heirAndKeySizeMap.put(split2[0], Integer.parseInt(split2[1]));
        }

        return heirAndKeySizeMap;
    }

    private static void readHierarchyFile(File hierarchyFile, int keySizeInBytes,
            List<ByteArrayHolder> byteArrayHolder) throws IOException {
        int rowLength = keySizeInBytes + 4;
        FileInputStream inputStream = null;
        FileChannel fileChannel = null;

        inputStream = new FileInputStream(hierarchyFile);
        fileChannel = inputStream.getChannel();

        long size = fileChannel.size();
        ByteBuffer rowlengthToRead = ByteBuffer.allocate(rowLength);
        try {
            while (fileChannel.position() < size) {
                fileChannel.read(rowlengthToRead);
                rowlengthToRead.rewind();

                byte[] mdKey = new byte[keySizeInBytes];
                rowlengthToRead.get(mdKey);
                int primaryKey = rowlengthToRead.getInt();
                byteArrayHolder.add(new ByteArrayHolder(mdKey, primaryKey));
                rowlengthToRead.clear();
            }
        } finally {
            MolapUtil.closeStreams(fileChannel, inputStream);
        }

    }

    /**
     * This method will copy input stream to output stream it will copy file to
     * destination
     *
     * @param stream    InputStream
     * @param outStream outStream
     * @throws IOException
     * @throws IOException IOException
     */
    private static void copyFile(InputStream stream, OutputStream outStream) throws IOException {
        int len;
        final byte[] buffer = new byte[MolapCommonConstants.BYTEBUFFER_SIZE];
        try {
            for (; ; ) {
                len = stream.read(buffer);
                if (len == -1) {
                    return;
                }
                outStream.write(buffer, 0, len);
            }
        } catch (IOException e) {
            throw e;
        } finally {
            MolapUtil.closeStreams(stream, outStream);
        }
    }

    /**
     * This method will copy input stream to output stream it will copy file to
     * destination and will not close the outputStream.
     *
     * @param stream    InputStream
     * @param outStream outStream
     * @throws IOException
     * @throws IOException IOException
     */
    private static void copyFileWithoutClosingOutputStream(InputStream stream,
            OutputStream outStream) throws IOException {

        final byte[] buffer = new byte[MolapCommonConstants.BYTEBUFFER_SIZE];
        int len;
        try {
            for (; ; ) {
                len = stream.read(buffer);
                if (len == -1) {
                    return;
                }
                outStream.write(buffer, 0, len);
            }
        } catch (IOException e) {
            throw e;
        } finally {
            MolapUtil.closeStreams(stream);
        }
    }

    /**
     * compare
     *
     * @param byte1
     * @param byte2
     * @return int
     */
    public static int compare(byte[] byte1, byte[] byte2) {
        int cmp = 0;
        int length = byte1.length;
        for (int i = 0; i < length; i++) {
            int a = (byte1[i] & 0xff);
            int b = (byte2[i] & 0xff);
            cmp = a - b;
            if (cmp != 0) {
                cmp = cmp < 0 ? -1 : 1;
                break;
            }
        }
        return cmp;
    }

    public static File decryptEncyptedFile(File memberFile) throws SliceMergerException {
        String filePath =
                memberFile.getAbsolutePath() + MolapCommonConstants.FILE_INPROGRESS_STATUS;
        return new File(filePath);
    }

    /**
     * below method will be used to get the files
     *
     * @param sliceLocation slocation locations
     * @return sorted files
     */
    public static MolapFile[] getSortedPathForFiles(String sliceLocation) {
        FileType fileType = FileFactory.getFileType(sliceLocation);
        MolapFile storeFolder = FileFactory.getMolapFile(sliceLocation, fileType);

        MolapFile[] files = storeFolder.listFiles(new MolapFileFilter() {

            @Override
            public boolean accept(MolapFile pathname) {
                if (!(pathname.isDirectory()) && pathname.getName().endsWith(".hierarchy")) {
                    return true;
                }
                return false;
            }
        });

        return MolapUtil.getSortedFileList(files);
    }

}
