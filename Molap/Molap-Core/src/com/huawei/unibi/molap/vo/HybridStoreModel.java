package com.huawei.unibi.molap.vo;

import java.util.Map;


public class HybridStoreModel
{

    /**
     * This array will have columns ordinal which are part of columnar store
     */
    private int[] columnStoreOrdinals;
    
    /**
     * This array will have columns ordinal which are part of row store
     */
    private int[] rowStoreOrdinals;
    

    /**
     * 
     */
    private int[] hybridCardinality;
    
    
    /**
     * This has detail of dimension which has to be stored as column based or row based
     * e.g {{1,1}
     *      {2,1}
     *      {3,1}
     *      {4,5,6}
     * it means first 3 dimension will be part of columnar store and next 3 dimension 
     * will be part of row store
     */
    private int[][] dimensionPartitioner;
    
    
    private int[] columnSplit;

    /**
     * It states whether store is hybrid store ie. data stored is both row and columnar
     */
    private boolean isHybridStore;
    

    /**
     * This will have mapping for dimension ordinal and its index in generated md keyArray
     * 
     */
    private Map<Integer, Integer> dimOrdinalMDKeymapping;
    
    
    /**
     * this will have dimension ordinal and its index in fact store
     */
    private Map<Integer, Integer> dimOrdinalStoreIndexMapping;

    
    private int noOfColumnsStore;
    

    public int[] getColumnStoreOrdinals()
    {
        return columnStoreOrdinals;
    }

    public void setColumnStoreOrdinals(int[] columnStoreOrdinals)
    {
        this.columnStoreOrdinals = columnStoreOrdinals;
    }

    public int[] getRowStoreOrdinals()
    {
        return rowStoreOrdinals;
    }

    public void setRowStoreOrdinals(int[] rowStoreOrdinals)
    {
        this.rowStoreOrdinals = rowStoreOrdinals;
    }

    public int[] getHybridCardinality()
    {
        return hybridCardinality;
    }

    public void setHybridCardinality(int[] hybridCardinality)
    {
        this.hybridCardinality = hybridCardinality;
    }

    public int[][] getDimensionPartitioner()
    {
        return dimensionPartitioner;
    }

    public void setDimensionPartitioner(int[][] dimensionPartitioner)
    {
        this.dimensionPartitioner = dimensionPartitioner;
    }

    public int[] getColumnSplit()
    {
        return columnSplit;
    }

    public void setColumnSplit(int[] split)
    {
        this.columnSplit = split;
    }

    public boolean isHybridStore()
    {
        return this.isHybridStore;
    }

    public void setHybridStore(boolean isHybridStore)
    {
        this.isHybridStore = isHybridStore;
    }

 
    public void setDimOrdinalMDKeyMapping(Map<Integer, Integer> dimOrdinalMDKeymapping)
    {
        this.dimOrdinalMDKeymapping=dimOrdinalMDKeymapping;
        
    }

    public int getMdKeyOrdinal(int ordinal)
    {
       return this.dimOrdinalMDKeymapping.get(ordinal);
        
    }

    public void setDimOrdinalStoreIndexMapping(Map<Integer, Integer> dimOrdinalStoreIndexMapping)
    {
       this.dimOrdinalStoreIndexMapping=dimOrdinalStoreIndexMapping;
        
    }
    
    public int getStoreIndex(int dimOrdinal)
    {
        return this.dimOrdinalStoreIndexMapping.get(dimOrdinal);
    }

    public void setNoOfColumnStore(int noOfColumnsStore)
    {
        this.noOfColumnsStore=noOfColumnsStore;
    }
    public int getNoOfColumnStore()
    {
        return this.noOfColumnsStore;
    }
    
}
