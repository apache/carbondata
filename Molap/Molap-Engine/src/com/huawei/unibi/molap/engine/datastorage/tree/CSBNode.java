/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOaJ/iBEdnA9gOR3ltflEugdkYXF8R4GN7ORAvkBD3HKrYFmDzSxjb64TO6c2vo3flNwG
BK1i8Vj2xgzLgxd8ZFS2ZDlGfZhJ8GHeEJkE27CXgE/KQrtGVmeefqkq4x0pvQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.datastorage.tree;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStoreBlock;
import com.huawei.unibi.molap.engine.schema.metadata.Pair;

/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :25-Jun-2013
 * FileName : CSBNode.java
 * Class Description : 
 * Version 1.0
 */
public abstract class CSBNode implements DataStoreBlock
{
    public abstract boolean isLeafNode();

    public abstract double[] getValue(int keyindex);

    public abstract CSBNode getChild(int childIndex);

    public abstract int getnKeys();

    public abstract CSBNode getNext();

    public abstract void setChildren(CSBNode[] children);

    public abstract void setNextNode(CSBNode nextNode);

    public abstract void setNext(CSBNode nextNode);

    public abstract void setKey(int keyindex, byte[] key);
    
    public abstract byte[] getKey(int keyIndex, FileHolder fileHolder);
    
    public abstract void addEntry(Pair<byte[], double[]> entry);
}
