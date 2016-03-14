/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3dxHTkgLvk/g0pMg4rifDcphjX7eJoAcGD1KT+ENR+HENX+P4jQiQxMWnQfbIVxL/bk2m
wy5zu1Q7rwGMyzWGPP3ZcgMuF1HiJaEIec6BJjNeXkjSc/xQQ+c/atlaoMoCig==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination.lru;


/**
 * @author R00900208
 *
 */
public class LRUCacheValue
{
    
    private long size;
    
    private long rowCount;
    
    private LRUCacheKey cacheKey;

    /**
     * @return the size
     */
    public long getSize()
    {
        return size;
    }

    /**
     * @param size the size to set
     */
    public void setSize(long size)
    {
        this.size = size;
    }

    /**
     * @return the rowCount
     */
    public long getRowCount()
    {
        return rowCount;
    }

    /**
     * @param rowCount the rowCount to set
     */
    public void setRowCount(long rowCount)
    {
        this.rowCount = rowCount;
    }

    /**
     * @return the cacheKey
     */
    public LRUCacheKey getCacheKey()
    {
        return cacheKey;
    }

    /**
     * @param cacheKey the cacheKey to set
     */
    public void setCacheKey(LRUCacheKey cacheKey)
    {
        this.cacheKey = cacheKey;
    }

}
