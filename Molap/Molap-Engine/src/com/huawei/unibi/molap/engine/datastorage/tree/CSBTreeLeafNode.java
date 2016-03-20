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

package com.huawei.unibi.molap.engine.datastorage.tree;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
import com.huawei.unibi.molap.datastorage.store.NodeKeyStore;
import com.huawei.unibi.molap.datastorage.store.NodeMeasureDataStore;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.util.StoreFactory;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.schema.metadata.Pair;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;

public class CSBTreeLeafNode extends CSBNode
{
    /**
     * Number of keys in the node
     */
    protected int nKeys;

    // Previous node
    // CSBNode prevNode;

    /**
     * Next node
     */
    private CSBNode nextNode;

    /**
     * Key array
     */
    protected NodeKeyStore keyStore;

    // Key array
    // byte [] [] keys;

    /**
     * Value array
     */
    private NodeMeasureDataStore dataStore;

    /**
     * @return the nKeys
     */
    public int getnKeys()
    {
        return nKeys;
    }

    /**
     * @param nKeys
     *            the nKeys to set
     */
    public void setnKeys(int nKeys)
    {
        this.nKeys = nKeys;
    }

    /**
     * @return the prevnode
     */
    // public CSBNode getPrevNode() {
    // return prevNode;
    // }

    /**
     * @param prevnode
     *            the prevnode to set
     */
    // public void setPrevNode(CSBNode prevNode) {
    // this.prevNode = prevNode;
    // }

    /**
     * @return the nextnode
     */
    public CSBNode getNext()
    {
        return nextNode;
    }

    /**
     * @param nextnode
     *            the nextnode to set
     */
    public void setNext(CSBNode nextNode)
    {
        this.nextNode = nextNode;
    }

//    /**
//     * @return the entries
//     */
//    public byte[] getMinKey()
//    {
//        return keyStore.get(0);
//    }
//
//    /**
//     * @return the entries
//     */
//    public byte[] getFirstKey()
//    {
//        return keyStore.get(0);
//    }
//
//    /**
//     * @return the entries
//     */
//    public byte[] getLastKey()
//    {
//        return keyStore.get(nKeys - 1);
//    }

    /**
     * @see com.huawei.unibi.molap.engine.datastorage.tree.CSBNode#addEntry(com.huawei.unibi.molap.engine.schema.metadata.Pair)
     */
    public void addEntry(Pair<byte[], double[]> entry)
    {
        keyStore.put(nKeys, entry.getKey());
        // values[nKeys] = entry.getValue();
//        dataStore.put(nKeys, entry.getValue());
        nKeys++;
    }

//    public CSBTreeLeafNode(int maxKeys, int keySizeInBytes, int valueCount,String filePath, boolean isFileStore, long offset, int length, FileHolder fileHolder, ValueCompressionModel compressionModel)
//    {
//        // TODO Auto-generated constructor stub
//        nKeys = 0;
//        // prevNode = null;
//        nextNode = null;
//        //keyStore = StoreFactory.createStore(maxKeys, keySizeInBytes, true, raFile, tableName, filePath);
//        keyStore= StoreFactory.createKeyStore(maxKeys, keySizeInBytes, true, isFileStore, offset, filePath, length, fileHolder);
//        //long [] measuresOffsets = new long [offset.length-1];
//        //System.arraycopy(offset, 1, measuresOffsets, 0, measuresOffsets.length);
//        //int [] measuresLength = new int [length.length-1];
//        //System.arraycopy(length, 1, measuresLength, 0, measuresLength.length);
//        //dataStore = StoreFactory.createDataStore(maxKeys, valueCount, raFile, tableName, filePath);
//        dataStore = StoreFactory.createDataStore(maxKeys, valueCount, isFileStore, compressionModel, measuresOffsets, measuresLength, filePath);
//        // values = new double[maxKeys][];
//    }

    public CSBTreeLeafNode(int maxKeys,int keySizeInBytes, int valueCount,boolean isFileStore,boolean isDataStoreReq) {
        // TODO Auto-generated constructor stub
     
//      prevNode = null;
      
        keyStore = StoreFactory.createKeyStore(maxKeys, keySizeInBytes,isFileStore);
        if(isDataStoreReq)
        {
            dataStore = StoreFactory.createDataStore(null);
        }
//      values = new double[maxKeys][];
    }
    public CSBTreeLeafNode(int maxKeys, int keySizeInBytes, boolean isFileStore, FileHolder fileHolder,
            LeafNodeInfo leafNodeInfo, ValueCompressionModel compressionModel)
    {
        nKeys = leafNodeInfo.getNumberOfKeys();
        keyStore = StoreFactory.createKeyStore(maxKeys, keySizeInBytes, true, isFileStore, leafNodeInfo.getKeyOffset(),
                leafNodeInfo.getFileName(), leafNodeInfo.getKeyLength(), fileHolder);
        dataStore = StoreFactory.createDataStore(isFileStore,
                compressionModel, leafNodeInfo.getMeasureOffset(), leafNodeInfo.getMeasureLength(),
                leafNodeInfo.getFileName(),fileHolder);
    }
         
    /**
     * @param keyindex
     *            the key number to set
     */
    public double[] getValue(int keyindex)
    {
        // return values[keyindex];
        return null;// dataStore.get(keyindex);
    }
    
    public boolean isLeafNode()
    {
        return true;
    }
    
    public void setChildren(CSBNode[] children)
    {
        // We shouldn't ever be here
    }

    @Override
    public KeyValue getNextKeyValue(int index)
    {
        // return new KeyValue<byte[], double[]>(keyStore.get(index),
        // values[index]);
        // return new KeyValue<byte[], double[]>(keyStore.get(index),
        // dataStore.get(index));
        return null;
    }
    
    /**
     * @param childIndex
     *            index of the child to be returned
     * @return the child node
     */
    public CSBNode getChild(int childIndex)
    {
        return null;
    }
    
    public void setKey(int keyindex, byte[] key)
    {

    }

    /**
     * Removes the last entry from the node.
     */
    public void removeLastEntry()
    {
        nKeys--;
    }

    /**
     * @param nextnode
     *            the nextnode to set
     */
    public void setNextNode(CSBNode nextNode)
    {
        this.nextNode = nextNode;
    }

  

    /**
     * Resets the first entry in the node to given value.
     * 
     */
    public void setFirstEntry(byte[] key, double[] value)
    {
        keyStore.put(0, key);
//        dataStore.put(0, value);
    }

//    @Override
//    public void compress(ValueCompressionModel compressionModel)
//    {
//        keyStore.compress();
//        dataStore.compress(compressionModel);
//    }

    @Override
    public byte[] getBackKeyArray(FileHolder fileHolder)
    {

        return keyStore.getBackArray(fileHolder);
    }

    @Override
    public short getValueSize()
    {
        // TODO Auto-generated method stub
        return dataStore.getLength();
    }

    @Override
    public MeasureDataWrapper getNodeMsrDataWrapper(int[] cols,FileHolder fileHolder)
    {
        return dataStore.getBackData(cols, fileHolder);
    }

    @Override
    public byte[] getKey(int keyIndex, FileHolder fileHolder)
    {
        return keyStore.get(keyIndex, fileHolder);
    }

    @Override
    public ColumnarKeyStoreDataHolder[] getColumnarKeyStore(FileHolder fileHolder, int[] blockIndex, boolean[] needCompressedData)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ColumnarKeyStoreDataHolder getColumnarKeyStore(FileHolder fileHolder, int blockIndex,
            boolean needCompressedData)
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public long getNodeNumber()
    {
        // TODO Auto-generated method stub
        return 0;
    }
    
    @Override
    public MeasureDataWrapper getNodeMsrDataWrapper(int cols, FileHolder fileHolder)
    {
        return dataStore.getBackData(cols, fileHolder);
    }

    @Override
    public byte[][] getBlockMaxData()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[][] getBlockMinData()
    {
        // TODO Auto-generated method stub
        return null;
    }
}
