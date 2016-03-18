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

import java.util.ArrayList;
import java.util.List;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.datastorage.store.impl.FileFactory;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStore;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStoreBlock;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
//import com.huawei.unibi.molap.engine.datastorage.Pair;
import com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream;
import com.huawei.unibi.molap.engine.scanner.Scanner;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.LeafNodeInfo;
import com.huawei.unibi.molap.metadata.LeafNodeInfoColumnar;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;
import com.huawei.unibi.molap.util.MolapProperties;
import com.huawei.unibi.molap.vo.HybridStoreModel;

/**
 * Cache Sensitive B+-Tree to implement a search structure that is stored
 * entirely in-memory and is efficient in terms CPU cache misses entailed in
 * search operation
 * 
 */
public class CSBTree implements DataStore
{
    // Maximum key value
    // private static int MAXIMUM_KEY;

    /**
     * Number of keys per page
     */
    private static final int DEFAULT_PAGESIZE = 32;

    /**
     * 
     */
//    private static final int CACHELINESIZE = 128;

    // Maximum number of keys in a Leaf node
    // private int maxKeys;

    /**
     * Maximum number of entries in leaf nodes
     */
    private int leafMaxEntry;

    /**
     * Maximum number of entries in upper nodes
     */
    private int upperMaxEntry;

    /**
     * Maximum children for upper nodes (intermediate nodes)
     */
    private int upperMaxChildren;

    /**
     * Number of leaf nodes
     */
    private int nLeaf;

    // Number of upper level nodes
    // private int nUpper;

    /**
     * Root of the tree
     */
    private CSBNode root;

    /**
     * 
     */
    private String tableName;

    /**
     * No. of keys that fit in a cache line
     */
 //   private int nCacheKeys;

    /**
     * 
     */
    private long[][] rangeValues;

    /**
     * 
     */
   // private List<Pair<long[], String>> rangeFiles;

    /**
     * Total number of entries in CSB-Tree
     */
    private long nTotalKeys;

    /**
     * 
     */
    private KeyGenerator keyGenerator;

    /**
     * The number of values stored for each key
     */
   // private int valueCount;

    /**
     * 
     */
    private long rangeSplitValue;

    /**
     * 
     */
    private int cpuUsagePercentage;

    /**
     * 
     */
    private ValueCompressionModel compressionModel;

    /**
     * 
     */
    private boolean isFileStore;
    
    /**
     * blockSize
     */
    private int[] keyBlockSize;
    
    private boolean [] aggKeyBlock;

    private HybridStoreModel hybridStoreModel;
    
    // private String dataFolderLoc;
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER = LogServiceFactory
            .getLogService(CSBTree.class.getName());
    

    // Constructor
    public CSBTree(HybridStoreModel hybridStoreModel,KeyGenerator keyGenerator, int valueCount, String tableName , boolean isFileStore,int[] keyBlockSize,boolean [] aggKeyBlock)
    {
        super();

       // this.valueCount = valueCount;
        this.keyGenerator = keyGenerator;
        this.tableName = tableName;
        this.hybridStoreModel = hybridStoreModel;

        // TODO Need to account for page headers and other fields
        upperMaxEntry = Integer.parseInt(MolapProperties.getInstance().getProperty(
                "com.huawei.datastore.internalnodesize", DEFAULT_PAGESIZE + ""));
        upperMaxChildren = upperMaxEntry;

        // TODO Need to account for page headers and other fields
        leafMaxEntry = Integer.parseInt(MolapProperties.getInstance().getProperty(MolapCommonConstants.LEAFNODE_SIZE,
                MolapCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL));

//        dataFolderLoc = MondrianProperties.instance().getProperty("com.huawei.datastore.datalocation", "D:/data");
       /* isFileStore = Boolean.parseBoolean(MondrianProperties.instance().getProperty(
                "com.huawei.datastore.isfileStore", "false"));*/
        this.isFileStore = isFileStore;
        this.keyBlockSize=keyBlockSize;
       setRangeSplitvalue();
        //System.out.println("Range Split value for parallel execution of a tree : " + rangeSplitValue);
        this.aggKeyBlock=aggKeyBlock;
    }
    
 // Constructor
    public CSBTree(KeyGenerator keyGenerator, int valueCount, String tableName , boolean isFileStore)
    {
        super();

       // this.valueCount = valueCount;
        this.keyGenerator = keyGenerator;
        this.tableName = tableName;

        // TODO Need to account for page headers and other fields
        upperMaxEntry = Integer.parseInt(MolapProperties.getInstance().getProperty(
                "com.huawei.datastore.internalnodesize", DEFAULT_PAGESIZE + ""));
        upperMaxChildren = upperMaxEntry;

        // TODO Need to account for page headers and other fields
        leafMaxEntry = Integer.parseInt(MolapProperties.getInstance().getProperty(MolapCommonConstants.LEAFNODE_SIZE,
                MolapCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL));

//        dataFolderLoc = MondrianProperties.instance().getProperty("com.huawei.datastore.datalocation", "D:/data");
       /* isFileStore = Boolean.parseBoolean(MondrianProperties.instance().getProperty(
                "com.huawei.datastore.isfileStore", "false"));*/
        this.isFileStore = isFileStore;
      //  nCacheKeys = 1;
        setRangeSplitvalue();
        //System.out.println("Range Split value for parallel execution of a tree : " + rangeSplitValue);
    }

/**
 * setRangeSplitvalue
 */
private void setRangeSplitvalue()
{
    try
    {
        rangeSplitValue = Long.parseLong(MolapProperties.getInstance().getProperty("molap.rangeSplitValue",
                "3500000"));
    }
    catch(NumberFormatException e)
    {
        rangeSplitValue = 6000000L;
    }

    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Range Split value for parallel execution of a tree : " + rangeSplitValue);
    //System.out.println("Range Split value for parallel execution of a tree : " + rangeSplitValue);

    try
    {
        cpuUsagePercentage = Integer.parseInt(MolapProperties.getInstance().getProperty(
                MolapCommonConstants.NUM_CORES, MolapCommonConstants.NUM_CORES_DEFAULT_VAL));
    }
    catch(NumberFormatException e)
    {
        cpuUsagePercentage = 2;
    }
    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Range Split value for parallel execution of a tree : " + rangeSplitValue);
}

    /**
     * @param keySize
     *            keySize in bytes
     * @param valueSize
     *            size of value field in bytes
     */
    public CSBTree(int keySize, int valueSize, int pageSize)
    {
        super();

        // TODO Need to account for page headers and other fields
        upperMaxEntry = pageSize;
        upperMaxChildren = upperMaxEntry;

        // TODO Need to account for page headers and other fields
        leafMaxEntry = Integer.parseInt(MolapProperties.getInstance().getProperty("com.huawei.datastore.leafnodesize",
                DEFAULT_PAGESIZE + ""));

        // Number of keys that will fit in a cacheline
       /* if(keySize >= CACHELINESIZE / 2)
        {
            nCacheKeys = 1;
        }
        else
        {
            // We will assume that keySize is rounded to nearest 4-bytes. Here
            // are different values
            // possible for number of keys in a cacheline for different key
            // sizes. We need hard coded
            // binary searches for all potential values of keys that can fit in
            // a cacheline
            // keySize - nCacheKeys
            // 4 32
            // 8 16
            // 12 10
            // 16 8
            // 20 6
            // 24 5
            // 32 4
            // 36,40 3
            // > 40 2
            nCacheKeys = CACHELINESIZE / keySize;
        }*/
    }

    // return number of key-value pairs in the CSB-tree
    public long size()
    {
        return nTotalKeys;
    }

    public void buildColumnar(List<DataInputStream> sources, boolean hasFactCount,Cube metaCube)
    {
        long num = 0;
        int groupCounter;
        int nInternal = 0;
        CSBNode curNode = null;
        CSBNode prevNode = null;
        ArrayList<CSBNode[]> nodeGroups = new ArrayList<CSBNode[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        CSBNode[] currentGroup = null;
        List<long[]> rangeVals = new ArrayList<long[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        FileHolder fileHolder = null;
        List<List<byte[]>> interNSKeyList= new ArrayList<List<byte[]>>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<byte[]> leafNSKeyList= null;
        compressionModel = sources.get(0).getValueCompressionMode();
        long st = System.currentTimeMillis();
        long nodeNumber=0;
        for(DataInputStream source : sources)
        {
            List<LeafNodeInfoColumnar> leafNodeInfoList = source.getLeafNodeInfoColumnar();
            //Coverity fix added null check
            if(null != leafNodeInfoList)
            {
                if(leafNodeInfoList.size() > 0)
                {
                    leafNodeInfoList.get(0).getFileName();
                    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Processing : " + (leafNodeInfoList.get(0).getFileName()) +" : "+(System.currentTimeMillis()-st));
                    st = System.currentTimeMillis();
                    
                }
                for(LeafNodeInfoColumnar leafNodeInfo : leafNodeInfoList)
                {
                        leafNodeInfo.setAggKeyBlock(aggKeyBlock);
                        num+=leafNodeInfo.getNumberOfKeys();
                        if(null == fileHolder)
                        {
                            fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(leafNodeInfo.getFileName()));
                        }
                        curNode = new CSBTreeColumnarLeafNode(leafNodeInfo.getNumberOfKeys(), keyBlockSize, isFileStore, fileHolder,
                                leafNodeInfo, compressionModel,nodeNumber++,metaCube,hybridStoreModel);
                        nLeaf++;
    
                        if(prevNode != null)
                        {
                            prevNode.setNextNode(curNode);
                        }
                        prevNode = curNode;
    
                        groupCounter = (nLeaf - 1) % (upperMaxChildren);
                        if(groupCounter == 0)
                        {
                            // Create new node group if current group is full
                            leafNSKeyList = new ArrayList<byte[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                            currentGroup = new CSBNode[upperMaxChildren];
                            nodeGroups.add(currentGroup);
                            nInternal++;
                            interNSKeyList.add(leafNSKeyList);
                        }
                        if(null != leafNSKeyList)
                        {
                            leafNSKeyList.add(leafNodeInfo.getStartKey());
                        }
                        if(null != currentGroup)
                        {
                            currentGroup[groupCounter] = curNode;
                        }
                        
                }
            }
        }

//        rangeVals = caclulateRanges(num, nodeGroups, rangeVals, fileHolder);
        if(num == 0)
        {
//            root = new CSBInternalNode(upperMaxEntry, keyGenerator.getKeySizeInBytes(), tableName);
            root = new CSBInternalNode(upperMaxEntry, keyGenerator.getStartAndEndKeySizeWithOnlyPrimitives(), tableName);
            return;
        }
        findCurrentNode(nInternal, curNode, nodeGroups, currentGroup, interNSKeyList);
        nTotalKeys = num;
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "*********************************************************Total Number Rows In BTREE: "+ nTotalKeys);
        this.rangeValues = (rangeVals.size() == 0) ? null : rangeVals.toArray(new long[rangeVals.size()][]);
        long compressionStart = System.currentTimeMillis();
        long compressionEnd = System.currentTimeMillis();
        if(null != fileHolder)
        {
            fileHolder.finish();
        }
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Compress Time:" + (compressionEnd - compressionStart) + "ms");
    }
    
    public void build(List<DataInputStream> sources, boolean hasFactCount)
    {
        long num = 0;
        int groupCounter;
        int nInternal = 0;
        CSBNode curNode = null;
        CSBNode prevNode = null;
        ArrayList<CSBNode[]> nodeGroups = new ArrayList<CSBNode[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        CSBNode[] currGroup = null;
        List<long[]> rangeVals = new ArrayList<long[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType());
        List<List<byte[]>> interNSKeyList= new ArrayList<List<byte[]>>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<byte[]> leafNSKeyList= null;
        compressionModel = sources.get(0).getValueCompressionMode();
        long st = System.currentTimeMillis();
        for(DataInputStream source : sources)
        {
            List<LeafNodeInfo> leafNodeInfoList = source.getLeafNodeInfo();
            //Coverity fix added null check
            if(null != leafNodeInfoList)
            {
                if(leafNodeInfoList.size() > 0)
                {
                    leafNodeInfoList.get(0).getFileName();
                    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Processing : " + (leafNodeInfoList.get(0).getFileName()) +" : "+(System.currentTimeMillis()-st));
                    st = System.currentTimeMillis();
                    
                }
                for(LeafNodeInfo leafNodeInfo : leafNodeInfoList)
                {
                        num+=leafNodeInfo.getNumberOfKeys();
                        curNode = new CSBTreeLeafNode(leafNodeInfo.getNumberOfKeys(), keyGenerator.getKeySizeInBytes(), isFileStore, fileHolder,
                                leafNodeInfo, compressionModel);
                        nLeaf++;
    
                        if(prevNode != null)
                        {
                            prevNode.setNextNode(curNode);
                        }
                        prevNode = curNode;
    
                        groupCounter = (nLeaf - 1) % (upperMaxChildren);
                        if(groupCounter == 0)
                        {
                            // Create new node group if current group is full
                            leafNSKeyList = new ArrayList<byte[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                            currGroup = new CSBNode[upperMaxChildren];
                            nodeGroups.add(currGroup);
                            nInternal++;
                            interNSKeyList.add(leafNSKeyList);
                        }
                        if(null != leafNSKeyList)
                        {
                            leafNSKeyList.add(leafNodeInfo.getStartKey());
                        }
                        if(null != currGroup)
                        { 
                            currGroup[groupCounter] = curNode;
                        }
                        
                }
            }
        }

        rangeVals = caclulateRanges(num, nodeGroups, rangeVals, fileHolder);
        if(num == 0)
        {
            root = new CSBInternalNode(upperMaxEntry, keyGenerator.getKeySizeInBytes(), tableName);
            return;
        }
        findCurrentNode(nInternal, curNode, nodeGroups, currGroup, interNSKeyList);
        nTotalKeys = num;
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "*********************************************************Total Number Rows In "+tableName+" : "+ nTotalKeys);
        this.rangeValues = (rangeVals.size() == 0) ? null : rangeVals.toArray(new long[rangeVals.size()][]);
        long compressionStart = System.currentTimeMillis();
        long compressionEnd = System.currentTimeMillis();
        fileHolder.finish();
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Compress Time:" + (compressionEnd - compressionStart) + "ms");
    }

    /**
     * @param nInternal
     * @param curNode
     * @param nodeGroups
     * @param currentGroup
     * @param interNSKeyList
     */
    private void findCurrentNode(int nInternal, CSBNode curNode, ArrayList<CSBNode[]> nodeGroups,
            CSBNode[] currentGroup, List<List<byte[]>> interNSKeyList)
    {
        int groupCounter;
        // Build internal nodes level by level. Each upper node can have
        // upperMaxEntry keys and upperMaxEntry+1 children
        int remainder;
        int nHigh;
        boolean bRootBuilt = false;
        ArrayList<CSBNode[]> childNodeGroups = nodeGroups;

        nHigh = nInternal;
        remainder = nLeaf % (upperMaxChildren);

        List<byte[]> interNSKeys= null;
        while(nHigh > 1 || !bRootBuilt)
        {
            ArrayList<CSBNode[]> internalNodeGroups = new ArrayList<CSBNode[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            List<List<byte[]>> interNSKeyTmpList= new ArrayList<List<byte[]>>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            nInternal = 0;
            for(int i = 0;i < nHigh;i++)
            {
                // Create a new internal node
//                curNode = new CSBInternalNode(upperMaxEntry, keyGenerator.getKeySizeInBytes(), tableName);
                curNode = new CSBInternalNode(upperMaxEntry, keyGenerator.getStartAndEndKeySizeWithOnlyPrimitives(), tableName);
                

                // Allocate a new node group if current node group is full
                groupCounter = i % (upperMaxChildren);
                if(groupCounter == 0)
                {
                    // Create new node group
                    currentGroup = new CSBInternalNode[upperMaxChildren];
                    internalNodeGroups.add(currentGroup);
                    nInternal++;
                    interNSKeys =new ArrayList<byte[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                    interNSKeyTmpList.add(interNSKeys);
                }

                // Add the new internal node to current group
                if(null != currentGroup)
                {
                    currentGroup[groupCounter] = curNode;
                }
                int nNodes;

                if(i == nHigh - 1 && remainder != 0)
                {
                    nNodes = remainder;
                }
                else
                {
                    nNodes = upperMaxEntry;
                }

                // Point the internal node to its children node group
                curNode.setChildren(childNodeGroups.get(i));

             //   int len = getLength(childNodeGroups.get(i));
                // Fill the internal node with keys based on its child nodes
                for(int j = 0;j < nNodes;j++)
                {
                    //curNode.setKey(j, childNodeGroups.get(i)[j + 1].getMinKey());
                    curNode.setKey(j, interNSKeyList.get(i).get(j));
                    if (j == 0 && null != interNSKeys)
                    {
                        interNSKeys.add(interNSKeyList.get(i).get(j));

                    }
                }
            }

            // If nHigh is 1, we have the root node
            if(nHigh == 1)
            {
                bRootBuilt = true;
            }

            remainder = nHigh % (upperMaxChildren);
            nHigh = nInternal;
            childNodeGroups = internalNodeGroups;
            interNSKeyList = interNSKeyTmpList;
        }
        root = curNode;
    }
    
    private List<long[]> caclulateRanges(long num, ArrayList<CSBNode[]> nodeGroups, List<long[]> rangeVals,
            FileHolder fileHolder)
    {
        //
        rangeSplitValue = num / cpuUsagePercentage;
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"New Range Split Value: " + rangeSplitValue);
        //System.out.println("New Range Split Value: " + rangeSplitValue);

        if(rangeSplitValue > 0)
        {
            CSBTreePart fullPart = new CSBTreePart();
            fullPart.internalNodeGroups = nodeGroups;
//            fullPart.nInternal = nodeGroups.size();
            fullPart.totalKeys = num;
//            fullPart.leafCount = nLeaf;
            //
            List<CSBTreePart> treePart = new ArrayList<CSBTree.CSBTreePart>(1);
            treePart.add(fullPart);
            processRanges(treePart, fullPart,fileHolder);
            rangeVals = fullPart.rangeVals;
        }
        return rangeVals;
    }


    // Search for given key, return associated entry; return null if no such key
    public KeyValue get(byte[] key, Scanner scanner)
    {
        return search(key, false, scanner);
    }

    // Search for given key, return associated entry if there is a match
    // Return next highest entry within the tree
    // Return null if the given is larger than the largest key value in the tree
    public KeyValue getNext(byte[] key, Scanner scanner)
    {
        return search(key, true, scanner);
    }

    //TODO SIMIAN
    /**
     * Search CSB tree for key. Returns null if there is no match. Uses standard
     * binary search
     * 
     * This method will first search in the Intermediate node to find the best
     * suitable leaf node then it will search in the leaf node to find the index
     * to start the scan
     * 
     * @param key
     * @return
     */
    public KeyValue search(byte[] key, boolean bNext, Scanner scanner)
    {
        //
        CSBNode node = root;
        if(null==node)
        {
            return null;
        }
        FileHolder fileHolder=null;
        if(null!=scanner)
        {
            fileHolder = scanner.getFileHolder();
        }
        else 
        {
            fileHolder = FileFactory.getFileHolder(FileFactory.getFileType());
        }
        int low = 0;
        int high = node.getnKeys() - 1; 
        int mid = 0;
        int compareRes = -1;
        byte[] bakArray =null;
        int len= 0; 
        //
        while(!node.isLeafNode())
        {
            node = binarySearchNonLeafNode(key, node, fileHolder);
        }
        // Do a binary search in the leaf node
        low = 0;
        mid = 0;
        compareRes = 0;
        bakArray = node.getBackKeyArray(fileHolder);
        len = keyGenerator.getKeySizeInBytes();
        while(low <= high)
        {
            //
            mid = (low + high) >>> 1;
            compareRes = keyGenerator.compare(key, 0, len, bakArray, ((mid) * len), len);
            if(compareRes < 0)
            {
                high = mid - 1;//
            }
            else if(compareRes > 0)
            {
                low = mid + 1;//
            }
            else
            {
                int currentPos = mid;
                while(currentPos-1 >= 0 && keyGenerator.compare(bakArray,((currentPos-1) * len),len, bakArray, ((currentPos) * len), len) == 0)
                {
                    currentPos--;
                }
                mid = currentPos;
                KeyValue entry = new KeyValue();
                entry.setKeyLength(keyGenerator.getKeySizeInBytes());
                entry.setBlock(node, bakArray, fileHolder);
                entry.setRow(mid);
                entry.setValueLength(node.getValueSize());
                if (null != scanner)
                {
                    scanner.setDataStore(this, node, mid);
                }
                else
                {
                    fileHolder.finish();
                }
                return entry;
            }
        }
        // No match found. The entry at mid should be the next highest key
        if(bNext)
        {
            if(compareRes > 0)
            {
                // The entry at mid is less than the input key. Advance it by
                // one
                mid++;
            }
            KeyValue keyValue = null;
            if(mid < node.getnKeys())
            {
                if(scanner != null)
                {
                    scanner.setDataStore(this, node, mid);
                }
                keyValue = new KeyValue();
                keyValue.setKeyLength(keyGenerator.getKeySizeInBytes());
                // keyValue.setBlock(node);
                keyValue.setBlock(node, bakArray,fileHolder);
                keyValue.setRow(mid);
                keyValue.setValueLength(node.getValueSize());
                return (keyValue);
            }
            else
            {
                if(scanner != null)
                {
                    scanner.setDataStore(this, node.getNext(), 0);
                }
                if(node.getNext() != null)
                {
                    keyValue = new KeyValue();
                    keyValue.setKeyLength(keyGenerator.getKeySizeInBytes());
                    // keyValue.setBlock(node);
                    keyValue.setBlock(node, bakArray,fileHolder);
                    keyValue.setRow(mid);
                    keyValue.setValueLength(node.getValueSize());
                    return (keyValue);
                }
            }
        }
        return null;
    }

    /**
     * 
     * @param key
     * @param node
     * @param fileHolder
     * @return
     * 
     */
    private CSBNode binarySearchNonLeafNode(byte[] key, CSBNode node, FileHolder fileHolder)
    {
        int childNodeIndex;
        int low;
        int high;
        int mid;
        int compareRsultVal; 
        byte[] bakArray;
        int length;
        // Do a binary search till we narrow down the search to a set of
        // keys
        // that will fit in a cacheline
        low = 0;
        high = node.getnKeys() - 1;
        mid = 0;
        compareRsultVal = -1;
        bakArray = node.getBackKeyArray(fileHolder);
        length = keyGenerator.getKeySizeInBytes();
        //
        while(low <= high)
        {
            mid = (low + high) >>> 1;
            compareRsultVal = keyGenerator.compare(key, 0, length, bakArray, ((mid) * length), length);
            if(compareRsultVal < 0)
            {
                high = mid - 1;//
            }
            else if(compareRsultVal > 0)
            {
                low = mid + 1;//
            }
            else
            {
                int currentPos = mid;
                while(currentPos-1 >= 0 && keyGenerator.compare(bakArray, ((currentPos-1) * length),length, bakArray, ((currentPos) * length), length) == 0)
                {
                    currentPos--;
                }
                mid = currentPos;
                break;
            }
        }
        //
        if(compareRsultVal < 0)
        {
            if(mid > 0)
            {
                mid--;
            }
            childNodeIndex = mid;
        }
        else
        {
            childNodeIndex = mid;
        }
        node = node.getChild(childNodeIndex);
        return node;
    }

    //TODO SIMIAN
    /**
     * 
     * @param key
     * @param node
     * @param fileHolder
     * @return
     * 
     */
    private CSBNode binarySearchNonLeafNodeFirstLeaf(byte[] key, CSBNode node, FileHolder fileHolder)
    {
        int childNodeIndex;
        int low;
        int high;
        int mid;
        int compareRes;
        byte[] bakArray;
        int length;
        // Do a binary search till we narrow down the search to a set of
        // keys
        // that will fit in a cacheline
        low = 0;
        high = node.getnKeys() - 1;
        mid = 0;
        compareRes = -1; 
        bakArray = node.getBackKeyArray(fileHolder);
        length = keyGenerator.getKeySizeInBytes();
        //
        while(low <= high)
        {
            mid = (low + high) >>> 1;
            compareRes = keyGenerator.compare(key, 0, length, bakArray, ((mid) * length), length);
            if(compareRes < 0)
            {
                high = mid - 1;//
            }
            else if(compareRes > 0)
            {
                low = mid + 1;//
            }
            else
            {
                int currentPos = mid;
                while(currentPos - 1 >= 0
                        && keyGenerator.compare(bakArray, ((currentPos - 1) * length), length, bakArray,
                                ((currentPos) * length), length) == 0)
                {
                    currentPos--;
                }
                mid = currentPos;
                break;
            }
        }
        //
        if(compareRes < 0)
        {
            if(mid > 0)
            {
                mid--;
            }
            childNodeIndex = mid;
        }
        else
        {
            childNodeIndex = mid;
        }
        node = node.getChild(childNodeIndex);
        return node;
    }
    
    /**
     * 
     * @param key
     * @param node
     * @param fileHolder
     * @return
     * 
     */
    private CSBNode binarySearchNonLeafNodeLastLeaf(byte[] key, CSBNode node, FileHolder fileHolder)
    {
        int childNodeIndex;
        int low;
        int high;
        int mid;
        int compareResult;
        byte[] bakArray;
        int length;
        int maxNumberOfElement = 0;
        // Do a binary search till we narrow down the search to a set of
        // keys
        // that will fit in a cacheline
        low = 0;
        high = node.getnKeys() - 1;
        maxNumberOfElement = high;
        mid = 0;
        compareResult = -1;
        bakArray = node.getBackKeyArray(fileHolder);
        length = keyGenerator.getKeySizeInBytes();
        //
        while(low <= high)
        {
            mid = (low + high) >>> 1;
            compareResult = keyGenerator.compare(key, 0, length, bakArray, ((mid) * length), length);
            if(compareResult < 0)
            {
                high = mid - 1;//
            }
            else if(compareResult > 0)
            {
                low = mid + 1;//
            }
            else
            {
                int currentPos = mid;
                while(currentPos + 1 <= maxNumberOfElement
                        && keyGenerator.compare(bakArray, ((currentPos + 1) * length), length, bakArray,
                                ((currentPos) * length), length) == 0)
                {
                    currentPos++;
                }
                mid = currentPos;
                break;
            }
        }
        //
        if(compareResult < 0)
        {
            if(mid > 0)
            {
                mid--;
            }
            childNodeIndex = mid;
        }
        else
        {
            childNodeIndex = mid;
        }
        node = node.getChild(childNodeIndex);
        return node;
    }
    
    @Override
    public long[][] getRanges()
    {
        if(null != rangeValues)
        {
            return rangeValues.clone();
//            temp1 = new long[rangeValues.length][rangeValues[0].length];
//            for(int i = 0;i < rangeValues.length;i++)
//            {
//                System.arraycopy(rangeValues[i], 0, temp1[i], 0, rangeValues[i].length);
//            }
        }
        return null;
    }


    /**
     * Project Name NSE V3R7C00 
     * Module Name : MOLAP
     * Author :C00900810
     * Created Date :25-Jun-2013
     * FileName : CSBTree.java
     * Class Description : 
     * Version 1.0
     */
    private class CSBTreePart
    {
        /**
         * 
         */
        private List<long[]> rangeVals = new ArrayList<long[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        /**
         * 
         */
        private long totalKeys;

        /**
         * 
         */
//        private int leafCount;

        /**
         * 
         */
        private ArrayList<CSBNode[]> internalNodeGroups = new ArrayList<CSBNode[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        /**
         * 
         */
//        private int nInternal;

        /**
         * 
         */
      //  private int emptyKeys;


        /**
         * 
         */
     //   private List<Pair<long[], String>> rangeFiles = new ArrayList<Pair<long[], String>>();

        /**
         * 
         */
      //  private RandomAccessFile raf;

        // for compression
        /**
         * 
         */
//        protected ValueCompressionModel valueModel;
    }


    /**
     * Returns the given key number from the the part. keyNmber expected to be
     * less than or equals to part size.
     */
    private byte[] getKeyFromPart(CSBTreePart part, long keyNumber,FileHolder fileHolder)
    {
        long maxKeysinGroup = upperMaxChildren * leafMaxEntry;

        // Identify the required group number and key index in group
        int addition = keyNumber % maxKeysinGroup == 0 ? 0 : 1;//CHECKSTYLE:OFF    Approval No:Approval-375,376
        int groupNumber = (int)(keyNumber / (maxKeysinGroup)) + addition;
        int keyNumInGroup = (int)(keyNumber - (groupNumber - 1) * maxKeysinGroup);
        CSBNode[] group = part.internalNodeGroups.get(groupNumber - 1);
//CHECKSTYLE:ON
        // Identify the required leaf node and key index in node
        addition = keyNumInGroup % leafMaxEntry == 0 ? 0 : 1;
        int leafNumber = keyNumInGroup / leafMaxEntry + addition;
        int keyNumInLeaf = keyNumInGroup - (leafNumber - 1) * leafMaxEntry;

        CSBTreeLeafNode leaf = (CSBTreeLeafNode)group[leafNumber - 1];
        if(keyNumInLeaf > leaf.getnKeys())
        {
            keyNumInLeaf = leaf.getnKeys();
        }
        // Identify the required key
        return leaf.getKey(keyNumInLeaf - 1, fileHolder);
    }


    public long getRangeSplitValue()
    {
        return rangeSplitValue;
    }


    /**
     * Identify the split ranges.
     * 
     * @param treeParts
     * @param count
     * @param fullPart
     * 
     */
    public void processRanges(List<CSBTreePart> treeParts, CSBTreePart fullPart, FileHolder fileHolder)
    {
        long leftOver = 0;
        long currentpartIndex = 0;

        for(int partNumber = 0;partNumber < treeParts.size();partNumber++)
        {
            CSBTreePart currentPart = treeParts.get(partNumber);
            currentpartIndex = 0;
            while(true)
            {
                // First time this part is entered.
                if(leftOver < rangeSplitValue)
                {
                    currentpartIndex = (rangeSplitValue - leftOver);

                    // If index is out of this part, break and continue to next
                    // path
                    if(currentpartIndex > currentPart.totalKeys)
                    {
                        leftOver += currentPart.totalKeys;
                        break;
                    }
                }
                else
                {
                    // Means some more ranges can be divided from same part.
                    currentpartIndex = currentpartIndex + rangeSplitValue;
                }

                // Identify correct key from this part:
                byte[] key = getKeyFromPart(currentPart, currentpartIndex,fileHolder);
                fullPart.rangeVals.add(keyGenerator.getKeyArray(key));

                // Find the left over keys and continue
                leftOver = currentPart.totalKeys - currentpartIndex;

                // If no more ranges in this part, continue to next part.
                if(leftOver < rangeSplitValue)
                {
                    break;
                }
            }
        }
    }


    @Override
    public ValueCompressionModel getCompressionModel()
    {
        return compressionModel;
    }

    @Override
    public void build(DataInputStream source, boolean hasFactCount)
    {
        int num = 0;
        int grpCounter;
        int nInternal = 0;
        CSBNode curNode = null;
        CSBNode prevNode = null;
        ArrayList<CSBNode[]> nodeGroups = new ArrayList<CSBNode[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        CSBNode[] currentGroup = null;
        List<long[]> rangeVals = new ArrayList<long[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType());
        List<List<byte[]>> interNSKeyList = new ArrayList<List<byte[]>>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        List<byte[]> leafNSKeyList = null;
        compressionModel = source.getValueCompressionMode();
        List<LeafNodeInfo> leafNodeInfoList = source.getLeafNodeInfo();
        //Coverity fix :Added null check
        if(null != leafNodeInfoList)
        {
            for(LeafNodeInfo leafNodeInfo : leafNodeInfoList)
            {
                num += leafNodeInfo.getNumberOfKeys();
                curNode = new CSBTreeLeafNode(num, keyGenerator.getKeySizeInBytes(), isFileStore, fileHolder, leafNodeInfo,
                        compressionModel);
                nLeaf++;
    
                if(prevNode != null)
                {
                    prevNode.setNextNode(curNode);
                }
                prevNode = curNode;
    
                grpCounter = (nLeaf - 1) % (upperMaxChildren);
                if(grpCounter == 0)
                {
                    // Create new node group if current group is full
                    leafNSKeyList = new ArrayList<byte[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                    currentGroup = new CSBNode[upperMaxChildren];
                    nInternal++;
                    nodeGroups.add(currentGroup);
                    interNSKeyList.add(leafNSKeyList);
                }
                if(null!=leafNSKeyList)
                {
                    leafNSKeyList.add(leafNodeInfo.getStartKey());
                }
                if(null != currentGroup)
                {
                    currentGroup[grpCounter] = curNode;
                }
            }
        }

        rangeVals = caclulateRanges(num, nodeGroups, rangeVals, fileHolder);
        if(num == 0)
        {
            root = new CSBInternalNode(upperMaxEntry, keyGenerator.getKeySizeInBytes(), tableName);
            return;
        }
        // Build internal nodes level by level. Each upper node can have
        // upperMaxEntry keys and upperMaxEntry+1 children
        int remainder;
        boolean bRootBuilt = false;
        int nHigh;
        ArrayList<CSBNode[]> childNodeGroups = nodeGroups;
        
        remainder = nLeaf % (upperMaxChildren);
        nHigh = nInternal;
        List<byte[]> interNSKeys = null;
        while(nHigh > 1 || !bRootBuilt)
        {
            ArrayList<CSBNode[]> internalNodeGroups = new ArrayList<CSBNode[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            List<List<byte[]>> interNSKeyTmpList = new ArrayList<List<byte[]>>(MolapCommonConstants.CONSTANT_SIZE_TEN);
            nInternal = 0;
            for(int k = 0;k < nHigh;k++)
            {
                // Create a new internal node
                curNode = new CSBInternalNode(upperMaxEntry, keyGenerator.getKeySizeInBytes(), tableName);

                // Allocate a new node group if current node group is full
                grpCounter = k % (upperMaxChildren);
                if(grpCounter == 0)
                {
                    // Create new node group
                    currentGroup = new CSBInternalNode[upperMaxChildren];
                    nInternal++;
                    internalNodeGroups.add(currentGroup);
                    interNSKeys = new ArrayList<byte[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
                    interNSKeyTmpList.add(interNSKeys);
                }

                // Add the new internal node to current group
                if(null!=currentGroup)
                {
                    currentGroup[grpCounter] = curNode;
                }
                
                int nNodes;

                if(k == nHigh - 1 && remainder != 0)
                {
                    nNodes = remainder - 1;
                }
                else
                {
                    nNodes = upperMaxEntry;
                }

                // Point the internal node to its children node group
                curNode.setChildren(childNodeGroups.get(k));

                // int len = getLength(childNodeGroups.get(i));
                // Fill the internal node with keys based on its child nodes
                List<byte[]> tmpList = null;
                for(int j = 0;j < nNodes;j++)
                {
                    // curNode.setKey(j, childNodeGroups.get(i)[j +
                    // 1].getMinKey());
                    tmpList = interNSKeyList.get(k);
                    if (null != tmpList)
                    {
                        curNode.setKey(j, tmpList.get(j + 1));
                        if(j == 0)
                        {
                            interNSKeys.add(tmpList.get(j + 1));
                        }
                    }
                }
            }

            // If nHigh is 1, we have the root node
            if(nHigh == 1)
            {
                bRootBuilt = true;
            }

            remainder = nHigh % (upperMaxChildren);
            nHigh = nInternal;
            childNodeGroups = internalNodeGroups;
            interNSKeyList = interNSKeyTmpList;
        }
        root = curNode;
        nTotalKeys = num;
        this.rangeValues = (rangeVals.size() == 0) ? null : rangeVals.toArray(new long[rangeVals.size()][]);
        long compressionStart = System.currentTimeMillis();
        long compressionEnd = System.currentTimeMillis();
        fileHolder.finish();
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Compress Time:" + (compressionEnd - compressionStart)
                + "ms");
    }

    @Override
    public DataStoreBlock getBlock(byte[] key, FileHolder fileHolder, boolean isFirst)
    {
        CSBNode node = root;
        if(isFirst)
        {
            while(!node.isLeafNode())
            {
                node = binarySearchNonLeafNodeFirstLeaf(key, node, fileHolder);
            }
        }
        else
        {
            while(!node.isLeafNode())
            {
                node = binarySearchNonLeafNodeLastLeaf(key, node, fileHolder);
            }
        }
        return node;
    }
}
