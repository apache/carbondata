/**
 * 
 */
package com.huawei.unibi.molap.engine.schema.metadata;

import java.util.Comparator;
import java.util.List;

import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.reader.ResultTempFileReader;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

/**
 * 
 * Project Name  : Carbon 
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : DataProcessorInfo.java
 * Description   : This class holds all the data required during sorting , merging , writing phase.
 * Class Version  : 1.0
 */
public class DataProcessorInfo
{

    private int keySize;

    private int fileBufferSize;

    private Comparator<ResultTempFileReader> heapComparator;
    
    private Comparator sortComparator;

    private KeyGenerator keyGenerator;

    private String cubeUniqueName;

    private boolean isSortedData;

    private String queryId;

    private int limit;
    
    /**
     * aggType
     */
    private String[] aggType;
    
    /**
     * msrMinValue
     */
    private double[] msrMinValue;

    /**
     * maskedByteRangeForsorting
     */
    private int[][] maskedByteRangeForSorting;

    /**
     * holderSize
     */
    private int holderSize;

    /**
     * dimensionSortOrder
     */
    private byte[] dimensionSortOrder;

    /**
     * dimensionMasks
     */
    private byte[][] dimensionMasks;

    /**
     * queryDims
     */
    private Dimension[] queryDims;

    /**
     * maskedByteRange
     */
    private int[] maskedByteRange;

    /**
     * actualMaskByteRanges
     */
    private int[] actualMaskByteRanges;

    /**
     * maxKey
     */
    private byte[] maxKey;

    /**
     * slices
     */
    private List<InMemoryCube> slices;

    private int blockSize;

    private byte[] sortedDimIndex;

    private boolean[] highCardinalityTypes;

    /**
     * @return the keySize
     */
    public int getKeySize()
    {
        return keySize;
    }

    /**
     * @param keySize the keySize to set
     */
    public void setKeySize(final int keySize)
    {
        this.keySize = keySize;
    }

    /**
     * @return the fileBufferSize
     */
    public int getFileBufferSize()
    {
        return fileBufferSize;
    }

    /**
     * @param fileBufferSize the fileBufferSize to set
     */
    public void setFileBufferSize(final int fileBufferSize)
    {
        this.fileBufferSize = fileBufferSize;
    }

    /**
     * @return the heapComparator
     */
    public Comparator<ResultTempFileReader> getHeapComparator()
    {
        return heapComparator;
    }

    /**
     * @param heapComparator the heapComparator to set
     */
    public void setHeapComparator(final Comparator<ResultTempFileReader> heapComparator)
    {
        this.heapComparator = heapComparator;
    }

    /**
     * @return the keyGenerator
     */
    public KeyGenerator getKeyGenerator()
    {
        return keyGenerator;
    }

    /**
     * @param keyGenerator the keyGenerator to set
     */
    public void setKeyGenerator(final KeyGenerator keyGenerator)
    {
        this.keyGenerator = keyGenerator;
    }

    /**
     * @return the cubeUniqueName
     */
    public String getCubeUniqueName()
    {
        return cubeUniqueName;
    }

    /**
     * @param cubeUniqueName the cubeUniqueName to set
     */
    public void setCubeUniqueName(final String cubeUniqueName)
    {
        this.cubeUniqueName = cubeUniqueName;
    }

    /**
     * @return the isSortedData
     */
    public boolean isSortedData()
    {
        return isSortedData;
    }

    /**
     * @param isSortedData the isSortedData to set
     */
    public void setSortedData(final boolean isSortedData)
    {
        this.isSortedData = isSortedData;
    }

    /**
     * @return the queryId
     */
    public String getQueryId()
    {
        return queryId;
    }

    /**
     * @param queryId the queryId to set
     */
    public void setQueryId(final String queryId)
    {
        this.queryId = queryId;
    }

    /**
     * @return the limit
     */
    public int getLimit()
    {
        return limit;
    }

    /**
     * @param limit the limit to set
     */
    public void setLimit(final int limit)
    {
        this.limit = limit;
    }

    /**
     * @return the maskedByteRangeForSorting
     */
    public int[][] getMaskedByteRangeForSorting()
    {
        return maskedByteRangeForSorting;
    }

    /**
     * @param maskedByteRangeForSorting the maskedByteRangeForSorting to set
     */
    public void setMaskedByteRangeForSorting(final int[][] maskedByteRangeForSorting)
    {
        this.maskedByteRangeForSorting = maskedByteRangeForSorting;
    }

    /**
     * @param holderSize the holderSize to set
     */
    public void setHolderSize(int holderSize)
    {
        this.holderSize = holderSize;
    }
    
    /**
     * @return the holderSize
     */
    public int getHolderSize()
    {
        return holderSize;
    }
    
    /**
     * @param dimensionSortOrder the dimensionSortOrder to set
     */
    public void setDimensionSortOrder(final byte[] dimensionSortOrder)
    {
        this.dimensionSortOrder = dimensionSortOrder;
    }

    /**
     * @return the dimensionSortOrder
     */
    public byte[] getDimensionSortOrder()
    {
        return dimensionSortOrder;
    }
    
    /**
     * @param dimensionMasks the dimensionMasks to set
     */
    public void setDimensionMasks(final byte[][] dimensionMasks)
    {
        this.dimensionMasks = dimensionMasks;
    }

    /**
     * @return the dimensionMasks
     */
    public byte[][] getDimensionMasks()
    {
        return dimensionMasks;
    }

    /**
     * @return the queryDims
     */
    public Dimension[] getQueryDims()
    {
        return queryDims;
    }

    /**
     * @param queryDims the queryDims to set
     */
    public void setQueryDims(final Dimension[] queryDims)
    {
        this.queryDims = queryDims;
    }

    /**
     * @return the maskedByteRange
     */
    public int[] getMaskedByteRange()
    {
        return maskedByteRange;
    }

    /**
     * @param maskedByteRange the maskedByteRange to set
     */
    public void setMaskedByteRange(final int[] maskedByteRange)
    {
        this.maskedByteRange = maskedByteRange;
    }

    /**
     * @return the actualMaskByteRanges
     */
    public int[] getActualMaskByteRanges()
    {
        return actualMaskByteRanges;
    }

    /**
     * @param actualMaskByteRanges the actualMaskByteRanges to set
     */
    public void setActualMaskByteRanges(final int[] actualMaskByteRanges)
    {
        this.actualMaskByteRanges = actualMaskByteRanges;
    }

    /**
     * @return the maxKey
     */
    public byte[] getMaxKey()
    {
        return maxKey;
    }

    /**
     * @param maxKey the maxKey to set
     */
    public void setMaxKey(final byte[] maxKey)
    {
        this.maxKey = maxKey;
    }

    /**
     * @return the slices
     */
    public List<InMemoryCube> getSlices()
    {
        return slices;
    }

    /**
     * @param slices the slices to set
     */
    public void setSlices(final List<InMemoryCube> slices)
    {
        this.slices = slices;
    }

    /**
     * @return the blockSize
     */
    public int getBlockSize()
    {
        return blockSize;
    }

    /**
     * @param blockSize the blockSize to set
     */
    public void setBlockSize(final int blockSize)
    {
        this.blockSize = blockSize;
    }

    /**
     * @return the sortComparator
     */
    public Comparator getSortComparator()
    {
        return sortComparator;
    }

    /**
     * @param sortComparator the sortComparator to set
     */
    public void setSortComparator(final Comparator sortComparator)
    {
        this.sortComparator = sortComparator;
    }

    public String[] getAggType()
    {
        return aggType;
    }

    public void setAggType(String[] aggType)
    {
        this.aggType = aggType;
    }

    public double[] getMsrMinValue()
    {
        return msrMinValue;
    }

    public void setMsrMinValue(double[] msrMinValue)
    {
        this.msrMinValue = msrMinValue;
    }

    public void setSortedDimensionIndex(byte[] sortedDimensionsIndex)
    {
        this.sortedDimIndex=sortedDimensionsIndex;
    }
    
    public byte[] getSortedDimensionIndex()
    {
        return sortedDimIndex;
    }

    public void setHighCardinalityTypes(boolean[] highCardinalityTypes)
    {
        this.highCardinalityTypes=highCardinalityTypes;
        
    }
    
    public boolean[] getHighCardinalityTypes()
    {
        return highCardinalityTypes;
    }

}
