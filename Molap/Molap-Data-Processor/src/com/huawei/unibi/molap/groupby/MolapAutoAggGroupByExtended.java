/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapAutoAggGroupBy.java
 * Class Description : Group by class to aggregate the measure value based on mdkey 
 * Class Version 1.0
 */
package com.huawei.unibi.molap.groupby;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : MolapAutoAggGroupByExtended.java
 * Class Description : Group by class to aggregate the measure value based on mdkey 
 * Class Version 1.0
 */
public class MolapAutoAggGroupByExtended extends MolapAutoAggGroupBy
{
    
    /**
     * MolapAutoAggGroupByExtended Constructor
     * @param aggType
     * @param aggClassName
     * @param schemaName
     * @param cubeName
     * @param tableName
     * @param factDims
     */
    public MolapAutoAggGroupByExtended(String[] aggType, String[] aggClassName,
            String schemaName, String cubeName, String tableName, int[] factDims, String extension, int currentRestructNum)
    {
        super(aggType,aggClassName,schemaName,cubeName,tableName,factDims,extension, currentRestructNum);
    }
    /**
     * Below method will be used to add new row
     * 
     * @param row
     * 
     */
    protected void addNewRow(Object[] row)
    {
        for(int i = 0;i < aggregators.length;i++)
        {
            if(null != row[i])
            {
                this.isNotNullValue[i]=true;
                aggregators[i].agg(row[i], (byte[])row[keyIndex],
                        0, 0);
            }
        }
        prvKey = (byte[])row[this.keyIndex];
        calculateMaxMinUnique();
    }

    /**
     * This method will be used to update the measure value based on aggregator
     * type
     * 
     * @param row
     *            row
     * 
     */
    protected void updateMeasureValue(Object[] row)
    {
        for(int i = 0;i < aggregators.length;i++)
        {
            if(null != row[i])
            {
                aggregators[i].agg(row[i], (byte[])row[keyIndex],
                        0, 0);
            }
        }
        calculateMaxMinUnique();
    }
}
