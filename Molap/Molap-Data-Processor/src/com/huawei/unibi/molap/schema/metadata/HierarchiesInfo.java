/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/vD3/dAALRhKgPGKyo/lto/vgv/KJrZ1cWb3Oe3AtLDG5XZga/GJp6kfIpM6pP5+2VDS
izmfslOEK3GPV64ApHhLCeitojCK1ZIp2qgA4KROZZSWSByvjARDjt4sl8Qtww==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.schema.metadata;

import java.util.Map;

/**
 * Project Name NSE V3R7C00 Module Name : MOLAP Author :C00900810 Created Date
 * :24-Jun-2013 FileName : HierarichiesInfo.java Class Description : Version 1.0
 */
public class HierarchiesInfo
{

    /**
     * hierarichieName
     */
    private String hierarichieName;
    
    /**
     * columnIndex
     */
    private int[] columnIndex;
    
    /**
     * columnNames
     */
    private String[] columnNames;
    
    /**
     * columnPropMap
     */
    private Map<String,String[]> columnPropMap;
    
    /**
     * loadToHierarichiTable
     */
    private boolean loadToHierarichiTable;
    
    /**
     * query
     */
    private String query;
    
    /**
     * Is Time Dimension
     */
    private boolean isTimeDimension;
    
    /**
     * levelTypeColumnMap
     */
    private Map<String, String> levelTypeColumnMap;
    
    /**
     * @return
     */
    public boolean isLoadToHierarichiTable()
    {
        return loadToHierarichiTable;
    }

    /**
     * @param loadToHierarichiTable
     */
    public void setLoadToHierarichiTable(boolean loadToHierarichiTable)
    {
        this.loadToHierarichiTable = loadToHierarichiTable;
    }

    /**
     * @return
     */
    public String getHierarichieName()
    {
        return hierarichieName;
    }

    /**
     * @param hierarichieName
     */
    public void setHierarichieName(String hierarichieName)
    {
        this.hierarichieName = hierarichieName;
    }

    /**
     * @return
     */
    public int[] getColumnIndex()
    {
        return columnIndex;
    }

    /**
     * @param columnIndex
     */
    public void setColumnIndex(int[] columnIndex)
    {
        this.columnIndex = columnIndex;
    }

    /**
     * @return
     */
    public String[] getColumnNames()
    {
        return columnNames;
    }

    /**
     * @param columnNames
     */
    public void setColumnNames(String[] columnNames)
    {
        this.columnNames = columnNames;
    }

    /**
     * @return
     */
    public Map<String, String[]> getColumnPropMap()
    {
        return columnPropMap;
    }

    /**
     * @param columnPropMap
     */
    public void setColumnPropMap(Map<String, String[]> columnPropMap)
    {
        this.columnPropMap = columnPropMap;
    }

    /**
     * 
     * @return Returns the query.
     * 
     */
    public String getQuery()
    {
        return query;
    }

    /**
     * 
     * @param query The query to set.
     * 
     */
    public void setQuery(String query)
    {
        this.query = query;
    }

    /**
     * 
     * @return Returns the isTimeDimension.
     * 
     */
    public boolean isTimeDimension()
    {
        return isTimeDimension;
    }

    /**
     * 
     * @param isTimeDimension The isTimeDimension to set.
     * 
     */
    public void setTimeDimension(boolean isTimeDimension)
    {
        this.isTimeDimension = isTimeDimension;
    }

    /**
     * 
     * @return Returns the levelTypeColumnMap.
     * 
     */
    public Map<String, String> getLevelTypeColumnMap()
    {
        return levelTypeColumnMap;
    }

    /**
     * 
     * @param levelTypeColumnMap The levelTypeColumnMap to set.
     * 
     */
    public void setLevelTypeColumnMap(Map<String, String> levelTypeColumnMap)
    {
        this.levelTypeColumnMap = levelTypeColumnMap;
    }

}
