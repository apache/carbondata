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

package com.huawei.unibi.molap.datastorage.store;

public class KeyStoreVO {
    /**
     * totalSize.
     */
    private int totalSize;

    /**
     * sizeOfEachElement.
     */
    private int sizeOfEachElement;

    /**
     * isLeaf.
     */
    private boolean isLeaf;

    /**
     * isFileStore.
     */
    private boolean isFileStore;

    /**
     * offset.
     */
    private long offset;

    /**
     * fileName.
     */
    private String fileName;

    /**
     * length.
     */
    private int length;

    /**
     * fileHolder.
     */
    private FileHolder fileHolder;


    /**
     * getTotalSize.
     *
     * @return int
     */
    public int getTotalSize() {
        return totalSize;
    }

    /**
     * setTotalSize.
     *
     * @param totalSize
     */
    public void setTotalSize(int totalSize) {
        this.totalSize = totalSize;
    }

    /**
     * getSizeOfEachElement
     *
     * @return int
     */
    public int getSizeOfEachElement() {
        return sizeOfEachElement;
    }

    /**
     * setSizeOfEachElement
     *
     * @param sizeOfEachElement
     */
    public void setSizeOfEachElement(int sizeOfEachElement) {
        this.sizeOfEachElement = sizeOfEachElement;
    }

    /**
     * isLeaf.
     *
     * @return boolean.
     */
    public boolean isLeaf() {
        return isLeaf;
    }

    /**
     * setLeaf.
     *
     * @param isLeaf
     */
    public void setLeaf(boolean isLeaf) {
        this.isLeaf = isLeaf;
    }

    /**
     * isFileStore()
     *
     * @return boolean.
     */
    public boolean isFileStore() {
        return isFileStore;
    }

    /**
     * setFileStore
     *
     * @param isFileStore boolean variable.
     */
    public void setFileStore(boolean isFileStore) {
        this.isFileStore = isFileStore;
    }

    /**
     * getOffset()
     *
     * @return long.
     */
    public long getOffset() {
        return offset;
    }

    /**
     * setOffset.
     *
     * @param offset
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    /**
     * getFileName
     *
     * @return String.
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * setFileName.
     *
     * @param fileName
     */
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    /**
     * getLength.
     *
     * @return int
     */
    public int getLength() {
        return length;
    }

    /**
     * setLength
     *
     * @param length
     */
    public void setLength(int length) {
        this.length = length;
    }

    /**
     * getFileHolder()
     *
     * @return FileHolder.
     */
    public FileHolder getFileHolder() {
        return fileHolder;
    }

    /**
     * setFileHolder.
     *
     * @param fileHolder
     */
    public void setFileHolder(FileHolder fileHolder) {
        this.fileHolder = fileHolder;
    }

}
