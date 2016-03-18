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

import java.nio.ByteBuffer;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
import com.huawei.unibi.molap.datastorage.store.NodeMeasureDataStore;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStore;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.util.StoreFactory;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.schema.metadata.Pair;
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.olap.MolapDef.CubeDimension;
import com.huawei.unibi.molap.util.MolapUtil;

public class CSBTreeColumnarLeafNode extends CSBNode
{
    
    /**
     * Number of keys in the node
     */
    private int nKeys;

    /**
     * Next node
     */
    private CSBNode nextNode;

    /**
     * Key array
     */
    private ColumnarKeyStore keyStore;
    
    /**
     * Value array
     */
    private NodeMeasureDataStore dataStore;
    
	/**
	 * Fact file name
	 */
    private String factFileName;
    
    /**
     * nodeNumber
     */
    private long nodeNumber;
    
    private byte[][] columnMinData;
    
    private byte[][] columnMaxData;
    
    public CSBTreeColumnarLeafNode(int maxKeys, int[] eachBlockSize, boolean isFileStore, FileHolder fileHolder,
            LeafNodeInfoColumnar leafNodeInfo, ValueCompressionModel compressionModel, long nodeNumber,Cube metaCube)
    {
        nKeys = leafNodeInfo.getNumberOfKeys();
        keyStore = StoreFactory.createColumnarKeyStore(MolapUtil.getColumnarKeyStoreInfo(leafNodeInfo, eachBlockSize), fileHolder,isFileStore);
        dataStore = StoreFactory.createDataStore(isFileStore,
                compressionModel, leafNodeInfo.getMeasureOffset(), leafNodeInfo.getMeasureLength(),
                leafNodeInfo.getFileName(),fileHolder);
        this.nodeNumber=nodeNumber;
        byte[][] columnMinMaxData=leafNodeInfo.getColumnMinMaxData();
        this.columnMinData=new byte[columnMinMaxData.length][];
        this.columnMaxData=new byte[columnMinMaxData.length][];
        com.huawei.unibi.molap.olap.MolapDef.Cube cubeXml=metaCube.getCube();
        CubeDimension[] cubeDimensions=cubeXml.dimensions;
        for(int i = 0;i < columnMinMaxData.length;i++)
        {

            //For high cardinality dimension engine has to ignore the length and store the min and max value
            //of dimension members.
            if(cubeDimensions[i].highCardinality || i >= eachBlockSize.length)
            {
                ByteBuffer byteBuffer = ByteBuffer.allocate(columnMinMaxData[i].length);
                byteBuffer.put(columnMinMaxData[i]);
                byteBuffer.flip();
                short minLength = byteBuffer.getShort();
                this.columnMinData[i] = new byte[minLength];
                byteBuffer.get(this.columnMinData[i]);
                short maxLength = byteBuffer.getShort();
                this.columnMaxData[i] = new byte[maxLength];
                byteBuffer.get(this.columnMaxData[i]);

            }
            else
            {
            this.columnMinData[i] = new byte[eachBlockSize[i]];
            System.arraycopy(columnMinMaxData[i], 0, this.columnMinData[i], 0, eachBlockSize[i]);

            this.columnMaxData[i] = new byte[eachBlockSize[i]];
            System.arraycopy(columnMinMaxData[i], eachBlockSize[i], this.columnMaxData[i], 0, eachBlockSize[i]);
            }
        }
        
        this.factFileName=leafNodeInfo.getFileName();
    }

    
    /**
     * @return the nKeys
     */
    public int getnKeys()
    {
        return nKeys;
    }

    
    public byte[][] getBlockMinData()
    {
        return this.columnMinData;
    }
    
    public byte[][] getBlockMaxData()
    {
        return this.columnMaxData;
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

    /**
     * @see com.huawei.unibi.molap.engine.datastorage.tree.CSBNode#addEntry(com.huawei.unibi.molap.engine.schema.metadata.Pair)
     */
    public void addEntry(Pair<byte[], double[]> entry)
    {
        
    }

    public boolean isLeafNode()
    {
        return true;
    }

    /**
     * @param keyindex
     *            the key number to set
     */
    public double[] getValue(int keyindex)
    {
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

    public void setChildren(CSBNode[] children)
    {
        // We shouldn't ever be here
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
        this.nextNode = nextNode;
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
     * Resets the first entry in the node to given value.
     * 
     */
    public void setFirstEntry(byte[] key, double[] value)
    {
    }

    @Override
    public byte[] getBackKeyArray(FileHolder fileHolder)
    {
        return null;
//        return keyStore.getBackArray(fileHolder);
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
        return null;
    }


    @Override
    public ColumnarKeyStoreDataHolder[] getColumnarKeyStore(FileHolder fileHolder, int[] blockIndex, boolean[] needCompressedData)
    {
        return keyStore.getUnCompressedKeyArray(fileHolder, blockIndex,needCompressedData);
    }



    @Override
    public long getNodeNumber()
    {
        // TODO Auto-generated method stub
        return nodeNumber;
    }


    @Override
    public ColumnarKeyStoreDataHolder getColumnarKeyStore(FileHolder fileHolder, int blockIndex,
            boolean needCompressedData)
    {
        return keyStore.getUnCompressedKeyArray(fileHolder, blockIndex,needCompressedData);
    }


    @Override
    public MeasureDataWrapper getNodeMsrDataWrapper(int cols, FileHolder fileHolder)
    {
        return dataStore.getBackData(cols, fileHolder);
    }
    
    public String getFactFile()
    {
        return factFileName;
    }


}
