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

public class RowTempFile {
    /**
     * row
     */
    private byte[] row;

    /**
     * file size
     */
    private long fileSize;

    /**
     * offset
     */
    private long offset;

    /**
     * file holder index
     */
    private int fileHolderIndex;

    /**
     * file path
     */
    private String filePath;

    /**
     * RowTempFile constructor
     *
     * @param row             row
     * @param fileSize        file size
     * @param offset          offset
     * @param fileHolderIndex file holder index
     * @param filePath        file path
     */
    public RowTempFile(byte[] row, long fileSize, long offset, int fileHolderIndex,
            String filePath) {
        this.row = row;
        this.fileSize = fileSize;
        this.offset = offset;
        this.fileHolderIndex = fileHolderIndex;
        this.filePath = filePath;
    }

    /**
     * get the row
     *
     * @return row
     */
    public byte[] getRow() {
        return row;
    }

    /**
     * set the row
     *
     * @param row
     */
    public void setRow(byte[] row) {
        this.row = row;
    }

    /**
     * Will return file size
     *
     * @return file size
     */
    public long getFileSize() {
        return fileSize;
    }

    /**
     * set the file size
     *
     * @param fileSize
     */
    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    /**
     * Will return offset
     *
     * @return offset
     */
    public long getOffset() {
        return offset;
    }

    /**
     * set the offset
     *
     * @param offset
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    /**
     * Will return file holder index
     *
     * @return fileHolderIndex
     */
    public int getFileHolderIndex() {
        return fileHolderIndex;
    }

    /**
     * set the file holder index
     *
     * @param fileHolderIndex
     */
    public void setFileHolderIndex(int fileHolderIndex) {
        this.fileHolderIndex = fileHolderIndex;
    }

    /**
     * return the file path
     *
     * @return filePath
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * set the file path
     *
     * @param filePath
     */
    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

}
