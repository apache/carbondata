/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwe/owl+XpObKvwejIomJrN10iZBX17jBC5vj/zP
61+XaY5k1dK4nyvvejorIUmStGrL1JQ0qjcCSwhfs8ubKPH2YyXwmSLrccc372doV/UghTjH
jcmIgwokM1TrpLG1jq8J7TegwnOn1O4ppSEzVKJfVUWCIYFJt8Tu60jEzeVHrA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.merger.sliceMerger;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM
 * FileName : DuplicateRecordHandler.java
 * Class Description :
 * Version 1.0
 */
public class DuplicateRecordHandler
{
    /**
     * leaf node size
     */
    private int leafNodeSize;

    /**
     * aggType
     */
    private String[] aggType;

    /**
     * mdkeyList
     */
    private List<byte[]> mdkeyList;

    /**
     * measuresList
     */
    private List<double[]> measuresList;

    /**
     * currentSize
     */
    private int currentSize;
    
    /**
     * currentUniqueValueToBeUsed
     */
    private double[] currentUniqueValueToBeUsed;

    /**
     * DuplicateRecordHandler Constructor
     * 
     * @param leafNodeSize
     *          leafNodeSize
     * @param aggType
     *          aggType
     *
     */
    public DuplicateRecordHandler(int leafNodeSize, String[] aggType, double[] currentUniqueValueToBeUsed)
    {
        this.leafNodeSize = leafNodeSize;
        this.aggType = aggType;
        this.mdkeyList = new ArrayList<byte[]>(leafNodeSize);
        this.measuresList = new ArrayList<double[]>(leafNodeSize);
        this.currentUniqueValueToBeUsed= currentUniqueValueToBeUsed;
    }

    /**
     * Below method will be used to clear the list
     *
     */
    public void clear()
    {
        this.mdkeyList = new ArrayList<byte[]>(leafNodeSize);
        this.measuresList = new ArrayList<double[]>(leafNodeSize);
        currentSize=0;
    }

    /**
     * Below method will be used to get the start key
     * 
     * @return first key of list
     *
     */
    public byte[] getStartKey()
    {
        return mdkeyList.get(0);
    }

    /**
     * below method will be used to get the end key
     * 
     * @return end key
     *
     */
    public byte[] getEndKey()
    {
        return mdkeyList.get(currentSize - 1);
    }

    /**
     * This method will be used to add tuple to list 
     * 
     * @param mdkey
     *          mdkey
     * @param msrs
     *          measures
     *
     */
    public void addList(byte[] mdkey, double[] msrs, double[] uniqueValue)
    {
        mdkeyList.add(mdkey);
        for(int i = 0;i < msrs.length;i++)
        {
            if(("max".equalsIgnoreCase(aggType[i]) || "min".equalsIgnoreCase(aggType[i])) && msrs[i] == uniqueValue[i])
            {
                msrs[i]= currentUniqueValueToBeUsed[i];
            }
        }
        measuresList.add(msrs);
        currentSize++;
    }

    /**
     * This method will be called for handling the duplicate keys
     * if same keys ke coming then we will update only the measures value
     * 
     * @param msrs
     *          measures
     *
     */
    public void updateMeasures(double[] msrs, double[] uniqueValue)
    {
        double[] ds = measuresList.get(currentSize - 1);
        for(int i = 0;i < ds.length;i++)
        {
            ds[i] = mapAggregateType(aggType[i], ds[i], msrs[i], uniqueValue[i],
                    currentUniqueValueToBeUsed[i]);
        }
        measuresList.set(currentSize - 1, ds);
    }

    /**
     * Below method will update the measure value based on its type
     * 
     * @param typeString
     *          agg type
     * @param firstValue
     *          first value
     * @param secValue
     *          second value
     * @return agg value
     *
     */
    private double mapAggregateType(String typeString, double firstValue, double secValue, double uniqueValue,
            double currentUniqueValueToBeUsed)
    {
        if("max".equalsIgnoreCase(typeString))
        {
            if(0 == Double.compare(currentUniqueValueToBeUsed, firstValue) && 0 == Double.compare(uniqueValue, secValue))
            {
                return firstValue;
            }
            else if(0 != Double.compare(currentUniqueValueToBeUsed, firstValue) && 0 == Double.compare(uniqueValue, secValue))
            {
                return firstValue;
            }
            else if (0 == Double.compare(currentUniqueValueToBeUsed, firstValue) && 0 != Double.compare(uniqueValue, secValue))
            {
                return secValue;
            }
            return firstValue > secValue ? firstValue : secValue;
        }
        else if("min".equalsIgnoreCase(typeString))
        {
            if(0 == Double.compare(currentUniqueValueToBeUsed, firstValue) && 0 == Double.compare(uniqueValue, secValue))
            {
                return firstValue;
            }
            else if(0 != Double.compare(currentUniqueValueToBeUsed, firstValue) && 0 == Double.compare(uniqueValue, secValue))
            {
                return firstValue;
            }
            else if (0 == Double.compare(currentUniqueValueToBeUsed, firstValue) && 0 != Double.compare(uniqueValue, secValue))
            {
                return secValue;
            }
            return firstValue < secValue ? firstValue : secValue;
        }
        else
        {
            return firstValue + secValue;
        }
    }

    /**
     * Below method will be used to get the mdkey list
     * 
     * @return mdkey list
     *
     */
    public List<byte[]> getMdkeyList()
    {
        return mdkeyList;
    }

    /**
     * below method will be used to get the measures list
     * 
     * @return measures list
     *
     */
    public List<double[]> getMeasuresList()
    {
        return measuresList;
    }

    /**
     * below method will be used to get the current size
     * 
     * @return currentSize
     *
     */
    public int getSize()
    {
        return currentSize;
    }
}
