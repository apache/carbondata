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

/**
 * 
 */
package com.huawei.unibi.molap.datastorage.store.columnar;

import java.util.Arrays;

/**
 * @author R00900208
 *
 */
public final class UnBlockIndexer
{
   // private static final LogService LOGGER = LogServiceFactory.getLogService(UnBlockIndexer.class.getName());

    private UnBlockIndexer()
    {
        
    }

    public static short[] uncompressIndex(short[] indexData, short[] indexMap)
    {
        int actualSize = indexData.length;
        for(int i = 0;i < indexMap.length;i++)
        {
            actualSize += indexData[indexMap[i]+1]-indexData[indexMap[i]]-1;
        }
        short[] indexes = new short[actualSize];
        int k = 0;
        for(short i = 0;i < indexData.length;i++)
        {
            int index = Arrays.binarySearch(indexMap, i);
            if(index > -1)
            {
                for(short j = indexData[indexMap[index]];j <= indexData[indexMap[index]+1];j++)
                {
                    indexes[k] = j;
                    k++;
                }
                i++;
            }
            else
            {
                indexes[k] = indexData[i];
                k++;
            }
        }
        return indexes;
    }
    
    public static int[] uncompressIndex(int[] indexData, int[] indexMap)
    {
        int actualSize = indexData.length;
        for(int i = 0;i < indexMap.length;i++)
        {
            actualSize += indexData[indexMap[i]+1]-indexData[indexMap[i]]-1;
        }
        int[] indexes = new int[actualSize];
        int k = 0;
        for(int i = 0;i < indexData.length;i++)
        {
            int index = Arrays.binarySearch(indexMap, i);
            if(index > -1)
            {
                for(int j = indexData[indexMap[index]];j <= indexData[indexMap[index]+1];j++)
                {
                    indexes[k] = j;
                    k++;
                }
                i++;
            }
            else
            {
                indexes[k] = indexData[i];
                k++;
            }
        }
        return indexes;
    }
    
    public static byte[][] uncompressData(byte[][] data, int[] indexMap)
    {
        int actualSize = data.length;
        for(int i = 1;i < indexMap.length;i+=2)
        {
            actualSize += indexMap[i]-1;
        }
        byte[][] uncompressedData = new byte[actualSize][];
        int k = 0;
        int l = 0;
        int mapLength = indexMap.length;
        for(int i = 0;i < data.length;i++)
        {
            if(l < mapLength && indexMap[l] == i)
            {
                for(int j = 0;j < indexMap[l+1];j++)
                {
                    uncompressedData[k] = data[i];
                    k++;
                }
                l+=2;
            }
            else
            {
                uncompressedData[k] = data[i];
                k++;
            }
        }
        return uncompressedData;
    }
    
    public static byte[][] uncompressData(byte[][] data, short[] indexMap)
    {
        int actualSize = data.length;
        for(int i = 1;i < indexMap.length;i+=2)
        {
            actualSize += indexMap[i]-1;
        }
        byte[][] uncompressedData = new byte[actualSize][];
        int k = 0;
        int l = 0;
        int mapLength = indexMap.length;
        for(short i = 0;i < data.length;i++)
        {
            if(l < mapLength && indexMap[l] == i)
            {
                for(short j = 0;j < indexMap[l+1];j++)
                {
                    uncompressedData[k] = data[i];
                    k++;
                }
                l+=2;
            }
            else
            {
                uncompressedData[k] = data[i];
                k++;
            }
        }
        return uncompressedData;
    }
    
//    public static byte[] uncompressData(byte[] data, int[] indexMap, int keyLen)
//    {
//        int dataLength = data.length/keyLen;
//        int actualSize = dataLength;
//        for(int i = 1;i < indexMap.length;i+=2)
//        {
//            actualSize += indexMap[i]-1;
//        }
//        byte[] uncompressedData = new byte[actualSize*keyLen];
//        int k = 0;
//        int l = 0;
//        int mapLength = indexMap.length;
//        for(int i = 0;i < dataLength;i++)
//        {
//            if(l < mapLength && indexMap[l] == i)
//            {
//                for(int j = 0;j < indexMap[l+1];j++)
//                {
//                    System.arraycopy(data, i*keyLen, uncompressedData, k*keyLen, keyLen);
//                    k++;
//                }
//                l+=2;
//            }
//            else
//            {
//                System.arraycopy(data, i*keyLen, uncompressedData, k*keyLen, keyLen);
//                k++;
//            }
//        }
//        return uncompressedData;
//    } 
    
    public static byte[] uncompressData(byte[] data , int[] index, int keyLen)
    {
        if(index.length<1)
        {
            return data;
        }
        int numberOfCopy=0;
        int actualSize=0;
        int srcPos=0;
        int destPos=0;
        for(int i = 1;i < index.length;i+=2)
        {
            actualSize += index[i];
        }
        byte[] uncompressedData = new byte[actualSize*keyLen];
        int picIndex=0;
        for(int i = 0;i < data.length;i+=keyLen)
        {
            numberOfCopy=index[picIndex*2+1];
            picIndex++;
            for(int j = 0;j < numberOfCopy;j++)
            {
                System.arraycopy(data, srcPos, uncompressedData, destPos, keyLen);
                destPos+=keyLen;
            }
            srcPos+=keyLen;
        }
        return uncompressedData;
    }
    
    public static byte[] uncompressData(byte[] data , short[] index, int keyLen)
    {
        if(index.length<1)
        {
            return data;
        }
        short numberOfCopy=0;
        int actualSize=0;
        int srcPos=0;
        int destPos=0;
        for(int i = 1;i < index.length;i+=2)
        {
            actualSize += index[i];
        }
        byte[] uncompressedData = new byte[actualSize*keyLen];
        for(short i = 0;i < data.length;i+=keyLen)
        {
            numberOfCopy=index[i*2+1];
            for(short j = 0;j < numberOfCopy;j++)
            {
                System.arraycopy(data, srcPos, uncompressedData, destPos, keyLen);
                destPos+=keyLen;
            }
            srcPos+=keyLen;
        }
        return uncompressedData;
    }
    
//    public static byte[] uncompressData(byte[] data, short[] indexMap, int keyLen)
//    {
//        int dataLength = data.length/keyLen;
//        int actualSize = dataLength;
//        for(int i = 1;i < indexMap.length;i+=2)
//        {
//            actualSize += indexMap[i]-1;
//        }
//        byte[] uncompressedData = new byte[actualSize*keyLen];
//        int k = 0;
//        int l = 0;
//        int mapLength = indexMap.length;
//        for(short i = 0;i < dataLength;i++)
//        {
//            if(l < mapLength && indexMap[l] == i)
//            {
//                for(short j = 0;j < indexMap[l+1];j++)
//                {
//                    System.arraycopy(data, i*keyLen, uncompressedData, k*keyLen, keyLen);
//                    k++;
//                }
//                l+=2;
//            }
//            else
//            {
//                System.arraycopy(data, i*keyLen, uncompressedData, k*keyLen, keyLen);
//                k++;
//            }
//        }
//        return uncompressedData;
//    } 
//    
   /* public static void main(String[] args)
    {
        byte[][] data = new byte[11][11];
        for(int i = 0;i <1;i++)
        {
            for(byte j = 0;j < data[i].length;j++)
            {
                data[i][j]=0;
            }
        }
        for(byte i = 1;i < data.length-1;i++)
        {
            for(byte j = 0;j < data[i].length;j++)
            {
                data[i][j]=j;
            }
        }
        
        for(int i = data.length-1;i <data.length;i++)
        {
            for(byte j = 0;j < data[i].length;j++)
            {
                data[i][j]=2;
            }
        }
        int size=0;
        for(int i = 0;i < data.length;i++)
        {
            size+=data[i].length; 
        }
        
        byte[] big1 = new byte[size];
        int dest=0;
        for(int i = 0;i < data.length;i++)
        {
            System.arraycopy(data[i], 0, big1, dest, data[i].length);
            dest+=data[i].length;
        } 
        BlockIndexerStorageForInt blockIndexerStorageForInt = new BlockIndexerStorageForInt(data,true);
        byte[][] keyBlock = blockIndexerStorageForInt.getKeyBlock();
        size=0;
        for(int i = 0;i < keyBlock.length;i++)
        {
            size+=keyBlock[i].length; 
        }
        byte[] big = new byte[size];
        dest=0;
        for(int i = 0;i < keyBlock.length;i++)
        {
            System.arraycopy(keyBlock[i], 0, big, dest, keyBlock[i].length);
            dest+=keyBlock[i].length;
        }        
        int[] dataIndexMap = blockIndexerStorageForInt.getDataIndexMap();
        LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.toString(dataIndexMap));
//        System.out.println(Arrays.toString(dataIndexMap));
        byte[] uncompressData = uncompressData(big, dataIndexMap, 11);
        LOGGER.info(MolapCoreLogEvent.UNIBI_MOLAPCORE_MSG, Arrays.equals(big1, uncompressData));
//        System.out.println(Arrays.equals(big1, uncompressData));
    }
*/
}
