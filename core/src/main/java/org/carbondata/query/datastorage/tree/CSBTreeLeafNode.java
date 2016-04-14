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

package org.carbondata.query.datastorage.tree;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.MeasureDataWrapper;
import org.carbondata.core.datastorage.store.NodeKeyStore;
import org.carbondata.core.datastorage.store.NodeMeasureDataStore;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.util.StoreFactory;
import org.carbondata.core.metadata.LeafNodeInfo;
import org.carbondata.query.datastorage.storeInterfaces.KeyValue;
import org.carbondata.query.schema.metadata.Pair;

public class CSBTreeLeafNode extends CSBNode {

    /**
     * Number of keys in the node
     */
    protected int nKeys;

    /**
     * Key array
     */
    protected NodeKeyStore keyStore;
    /**
     * Next node
     */
    private CSBNode nextNode;

    /**
     * Value array
     */
    private NodeMeasureDataStore dataStore;

    public CSBTreeLeafNode(int maxKeys, int keySizeInBytes, int valueCount, boolean isFileStore,
            boolean isDataStoreReq) {

        keyStore = StoreFactory.createKeyStore(maxKeys, keySizeInBytes, isFileStore);
        if (isDataStoreReq) {
            dataStore = StoreFactory.createDataStore(null);
        }
    }

    public CSBTreeLeafNode(int maxKeys, int keySizeInBytes, boolean isFileStore,
            FileHolder fileHolder, LeafNodeInfo leafNodeInfo,
            ValueCompressionModel compressionModel) {
        nKeys = leafNodeInfo.getNumberOfKeys();
        keyStore = StoreFactory.createKeyStore(maxKeys, keySizeInBytes, true, isFileStore,
                leafNodeInfo.getKeyOffset(), leafNodeInfo.getFileName(),
                leafNodeInfo.getKeyLength(), fileHolder);
        dataStore = StoreFactory
                .createDataStore(isFileStore, compressionModel, leafNodeInfo.getMeasureOffset(),
                        leafNodeInfo.getMeasureLength(), leafNodeInfo.getFileName(), fileHolder);
    }

    /**
     * @return the nKeys
     */
    public int getnKeys() {
        return nKeys;
    }

    /**
     * @param nKeys the nKeys to set
     */
    public void setnKeys(int nKeys) {
        this.nKeys = nKeys;
    }

    /**
     * @return the nextnode
     */
    public CSBNode getNext() {
        return nextNode;
    }

    public void setNext(CSBNode nextNode) {
        this.nextNode = nextNode;
    }

    /**
     * @see CSBNode#addEntry(Pair)
     */
    public void addEntry(Pair<byte[], double[]> entry) {
        keyStore.put(nKeys, entry.getKey());
        // values[nKeys] = entry.getValue();
        //        dataStore.put(nKeys, entry.getValue());
        nKeys++;
    }

    /**
     * @param keyindex the key number to set
     */
    public double[] getValue(int keyindex) {
        return null;
    }

    public boolean isLeafNode() {
        return true;
    }

    public void setChildren(CSBNode[] children) {
        // We shouldn't ever be here
    }

    @Override
    public KeyValue getNextKeyValue(int index) {
        return null;
    }

    /**
     * @param childIndex index of the child to be returned
     * @return the child node
     */
    public CSBNode getChild(int childIndex) {
        return null;
    }

    public void setKey(int keyindex, byte[] key) {

    }

    /**
     * Removes the last entry from the node.
     */
    public void removeLastEntry() {
        nKeys--;
    }

    public void setNextNode(CSBNode nextNode) {
        this.nextNode = nextNode;
    }

    /**
     * Resets the first entry in the node to given value.
     */
    public void setFirstEntry(byte[] key, double[] value) {
        keyStore.put(0, key);
    }

    @Override
    public byte[] getBackKeyArray(FileHolder fileHolder) {

        return keyStore.getBackArray(fileHolder);
    }

    @Override
    public short getValueSize() {
        return dataStore.getLength();
    }

    @Override
    public MeasureDataWrapper getNodeMsrDataWrapper(int[] cols, FileHolder fileHolder) {
        return dataStore.getBackData(cols, fileHolder);
    }

    @Override
    public byte[] getKey(int keyIndex, FileHolder fileHolder) {
        return keyStore.get(keyIndex, fileHolder);
    }

    @Override
    public ColumnarKeyStoreDataHolder[] getColumnarKeyStore(FileHolder fileHolder, int[] blockIndex,
            boolean[] needCompressedData,int[] noDictionaryVals) {
        return null;
    }

    @Override
    public ColumnarKeyStoreDataHolder getColumnarKeyStore(FileHolder fileHolder, int blockIndex,
            boolean needCompressedData,int[] noDictionaryVals) {
        return null;
    }

    @Override
    public long getNodeNumber() {
        return 0;
    }

    @Override
    public MeasureDataWrapper getNodeMsrDataWrapper(int cols, FileHolder fileHolder) {
        return dataStore.getBackData(cols, fileHolder);
    }

    @Override
    public byte[][] getBlockMaxData() {
        return null;
    }

    @Override
    public byte[][] getBlockMinData() {
        return null;
    }
}
