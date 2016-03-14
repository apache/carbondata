/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d0/3+mkJ6cIeSiU8ZpdJlfRNSGLseKB6XHiPq7/zhfHfGNAVuqRy9E2EklWbLVl7/e9a
Iw9MjrpjJu75S8mQfGVpFVB3+0BSCpB6IuyyIJiqR+w64KGqkrtPFJGRGchjcQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination;

import java.util.List;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * @author R00900208
 *
 */
public class Page
{
    private List<byte[]> keys;
    
    private List<MeasureAggregator[]> msrs;
    
    

    public Page(List<byte[]> keys, List<MeasureAggregator[]> msrs)
    {
        super();
        this.keys = keys;
        this.msrs = msrs;
    }

    /**
     * @return the keys
     */
    public List<byte[]> getKeys()
    {
        return keys;
    }

    /**
     * @return the msrs
     */
    public List<MeasureAggregator[]> getMsrs()
    {
        return msrs;
    }
    
    
}
