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
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import org.carbondata.core.datastorage.util.StoreFactory;
import org.carbondata.query.datastorage.storeInterfaces.KeyValue;
import org.carbondata.query.schema.metadata.Pair;

/**
 * Internal node of a cache sensitive B+Tree
 */
public class CSBInternalNode extends CSBNode {
    /**
     * number of keys in the node
     */
    private int nKeys;

    /**
     * Child nodes
     */
    private CSBNode[] children;

    /**
     * Key array
     */
    private NodeKeyStore keyStore;

    /**
     *
     */
    public CSBInternalNode(int maxKeys, int keySizeInBytes, String tableName) {
        // Create a new internal node with space for maxKeys
        keyStore = StoreFactory.createKeyStore(maxKeys, keySizeInBytes, false);
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
     * @return the children
     */
    public CSBNode[] getChildren() {
        return children;
    }

    /**
     * @param children the children to set
     */
    public void setChildren(CSBNode[] children) {
        this.children = children;
    }

    /**
     * @param childIndex index of the child to be returned
     * @return the child node
     */
    public CSBNode getChild(int childIndex) {
        return children[childIndex];
    }

    public double[] getValue(int keyindex) {
        // We shouldn't ever be here
        return null;
    }

    /**
     * @param keyindex the key number to set
     */
    public void setKey(int keyindex, byte[] key) {
        keyStore.put(keyindex, key);
        // this.keys[keyindex] = key;
        nKeys++;
    }

    /**
     * @see CSBNode#isLeafNode()
     */
    public boolean isLeafNode() {
        return false;
    }

    public void setPrevNode(CSBNode prevNode) {
        // We shouln't ever be here
    }

    /**
     * @return the next node
     */
    public CSBNode getNext() {
        return null;
    }

    public void setNext(CSBNode nextNode) {
        // We shouldn't ever be here
    }

    @Override
    public KeyValue getNextKeyValue(int index) {
        return null;
    }

    public void setNextNode(CSBNode nextNode) {
        // We shouldn't ever be here
    }

    @Override
    public byte[] getBackKeyArray(FileHolder fileHolder) {
        return keyStore.getBackArray(fileHolder);
    }

    @Override
    public short getValueSize() {
        return 0;
    }

    @Override
    public MeasureDataWrapper getNodeMsrDataWrapper(int[] cols, FileHolder fileHolder) {
        return null;
    }

    @Override
    public byte[] getKey(int keyIndex, FileHolder fileHolder) {
        return keyStore.get(keyIndex, fileHolder);
    }

    @Override
    public void addEntry(Pair<byte[], double[]> entry) {

    }

    @Override
    public ColumnarKeyStoreDataHolder[] getColumnarKeyStore(FileHolder fileHolder, int[] blockIndex,
            boolean[] needCompressedData,int[] noDictionaryVals) {
        return null;
    }

    @Override
    public long getNodeNumber() {
        return 0;
    }

    @Override
    public ColumnarKeyStoreDataHolder getColumnarKeyStore(FileHolder fileHolder, int blockIndex,
            boolean needCompressedData,int[] noDictionaryVals) {
        return null;
    }

    @Override
    public MeasureDataWrapper getNodeMsrDataWrapper(int cols, FileHolder fileHolder) {
        return null;
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
