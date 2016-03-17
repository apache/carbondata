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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBpz/elmRQGqbxO8KHRE4L50zFHn8f0JbFuMFnZHxFddLA9H5gtBszK4dGjnzJH09wlPr
L/bRPufBK9pA0WPiJBVUTdZzhd/lKL1D4SixYgAVZ+AdEaew3nqyecw/l2L0bQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.datastorage.tree;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
import com.huawei.unibi.molap.datastorage.store.NodeKeyStore;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.util.StoreFactory;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.schema.metadata.Pair;
/**
 * Internal node of a cache sensitive B+Tree
 * 
 * @author R00904487
 * 
 */
public class CSBInternalNode extends CSBNode
{
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
    public CSBInternalNode(int maxKeys, int keySizeInBytes, String tableName)
    {
        // Create a new internal node with space for maxKeys
     //   keyStore = StoreFactory.createStore(maxKeys, keySizeInBytes, false, null, tableName, null);
        keyStore = StoreFactory.createKeyStore(maxKeys, keySizeInBytes, false);
    }

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
     * @return the children
     */
    public CSBNode[] getChildren()
    {
        return children;
    }

    /**
     * @return the entries
     */
//    public byte[] getFirstKey()
//    {
//        // return keys[0];
//        return keyStore.get(0);
//    }
//
//    /**
//     * @return the entries
//     */
//    public byte[] getLastKey()
//    {
//        // return keys[nKeys - 1];
//        return keyStore.get(nKeys - 1);
//    }

    /**
     * @param children
     *            the children to set
     */
    public void setChildren(CSBNode[] children)
    {
        this.children = children;
    }

    /**
     * @param childIndex
     *            index of the child to be returned
     * @return the child node
     */
    public CSBNode getChild(int childIndex)
    {
        return children[childIndex];
    }

    /**
     * @return the keys
     */
    // public byte[][] getKeys()
    // {
    // return keys;
    // }

    /**
     * @param keyindex
     *            the key number to set
     */


    /**
     * @param keyindex
     *            the key number to set
     */
    public double[] getValue(int keyindex)
    {
        // We shouldn't ever be here
        return null;
    }

    /**
     * @param keys
     *            the keys to set
     */
    // public void setKeys(byte[][] keys)
    // {
    // this.keys = keys;
    // }

    /**
     * @param keyindex
     *            the key number to set
     */
    public void setKey(int keyindex, byte[] key)
    {
        keyStore.put(keyindex, key);
        // this.keys[keyindex] = key;
        nKeys++;
    }

    /**
     * 
     * @see com.huawei.unibi.molap.engine.datastorage.tree.CSBNode#isLeafNode()
     */
    public boolean isLeafNode()
    {
        return false;
    }

    /**
     * @param prevnode
     *            the prevnode to set
     */
    public void setPrevNode(CSBNode prevNode)
    {
        // We shouln't ever be here
    }

    /**
     * @param nextnode
     *            the nextnode to set
     */
    public void setNext(CSBNode nextNode)
    {
        // We shouldn't ever be here
    }

    /**
     * @return the next node
     */
    public CSBNode getNext()
    {
        return null;
    }

    @Override
    public KeyValue getNextKeyValue(int index)
    {
        return null;
    }

    /**
     * @param nextnode
     *            the nextnode to set
     */
    public void setNextNode(CSBNode nextNode)
    {
        // We shouldn't ever be here
    }

    /*
     * public void addEntry(KeyValue<byte[], double[]> entry) { }
     */


    @Override
    public byte[] getBackKeyArray(FileHolder fileHolder)
    {
        return keyStore.getBackArray(fileHolder);
    }

    @Override
    public short getValueSize()
    {
        return 0;
    }

    @Override
    public MeasureDataWrapper getNodeMsrDataWrapper(int[] cols,FileHolder fileHolder)
    {
        return null;
    }

    @Override
    public byte[] getKey(int keyIndex, FileHolder fileHolder)
    {
        return keyStore.get(keyIndex,fileHolder);
    }

    @Override
    public void addEntry(Pair<byte[], double[]> entry)
    {
        
    }

    @Override
    public ColumnarKeyStoreDataHolder[] getColumnarKeyStore(FileHolder fileHolder, int[] blockIndex,boolean[] needCompressedData)
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
    public ColumnarKeyStoreDataHolder getColumnarKeyStore(FileHolder fileHolder, int blockIndex,
            boolean needCompressedData)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MeasureDataWrapper getNodeMsrDataWrapper(int cols, FileHolder fileHolder)
    {
        return null;
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
