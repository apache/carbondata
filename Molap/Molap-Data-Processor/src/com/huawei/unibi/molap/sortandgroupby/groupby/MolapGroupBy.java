/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/uI0vMJfgxjZFPk9JUbp9Z7/q8MJ23Yrdexr7aWguu74hf67zNypIsRMh3EqMboTfYg+
QWt0F5w5GOcbbtOL4YtOyztdKGkYp3xoi0NAj8SbPdtHeKdUDWVOMpzlggFjhA==*/
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
package com.huawei.unibi.molap.sortandgroupby.groupby;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.sortandgroupby.exception.MolapSortKeyAndGroupByException;
import com.huawei.unibi.molap.util.MolapDataProcessorLogEvent;
import com.huawei.unibi.molap.util.MolapDataProcessorUtil;
import com.huawei.unibi.molap.util.MolapUtil;
import com.huawei.unibi.molap.util.MolapUtilException;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName : MolapGroupBy.java 
 * Class Description : This class will be used for merging the same key (Group By)
 * Version 1.0
 */
public class MolapGroupBy
{
    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapGroupBy.class.getName());
    /**
     * key array index
     */
    private int keyIndex;

    /**
     * column index mapping
     */
    private int[] columnIndexMapping;

    /**
     * aggregate type
     */
    private String[] aggType;

    /**
     * previous row
     */
    private Object[] prvRow;

    /**
     * previous row key
     */
    private byte[] prvKey;

    /**
     * max value for each measure
     */
    private double[] maxValue;

    /**
     * min value for each measure
     */
    private double[] minValue;

    /**
     * decimal length of each measure
     */
    private int[] decimalLength;

    /**
     * uniqueValue
     */
    private double[] uniqueValue;

    /**
     * channel
     */
    private DataOutputStream writeStream;

    /**
     * tmpFile
     */
    private File tmpFile;

    /**
     * readingStream
     */
    private DataInputStream readingStream;

    /**
     * numberOfEntries
     */
    private int numberOfEntries;

    /**
     * readCounter
     */
    private int readCounter;

    /**
     * mdKeyLength
     */
    private int mdKeyLength;

    /**
     * storeLocation
     */
    private String storeLocation;
    /**
     * 
     * 
     * @param aggType
     *            agg type
     * @param rowMeasures
     *            row Measures name
     * @param actual
     *            Measures actual Measures
     * @param row
     *            row
     * 
     */
    public MolapGroupBy(String aggType, String rowMeasures, String actualMeasures, Object[] row)
    {
        this.aggType = aggType.split(";");
        this.keyIndex = row.length - 1;
        this.columnIndexMapping = new int[this.aggType.length];
        updateColumnIndexMapping(rowMeasures.split(";"), actualMeasures.split(";"));
        this.maxValue = new double[this.aggType.length];
        this.minValue = new double[this.aggType.length];
        this.decimalLength = new int[this.aggType.length];
        this.uniqueValue = new double[this.aggType.length];
        for(int i = 0;i < this.aggType.length;i++)
        {
            maxValue[i] = -Double.MAX_VALUE;
            minValue[i] = Double.MAX_VALUE;
            decimalLength[i] = 0;
        }
        this.mdKeyLength= ((byte[])row[row.length-1]).length;
        addNewRow(row);
    }

    /**
     * Below method will be used to initialize 
     * 
     * @param storeLocation
     *          store location 
     * @param tableName
     *          table name 
     * @throws MolapSortKeyAndGroupByException
     *          any problem while initializing 
     *
     */
    public void initialize(String storeLocation, String tableName) throws MolapSortKeyAndGroupByException
    {
        // get the base location
        String baseLocation = MolapUtil.getCarbonStorePath(null, null)/*MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION,
                MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)*/;
        // update the store location 
        this.storeLocation = baseLocation + File.separator + storeLocation + File.separator
                + MolapCommonConstants.GROUP_BY_TEMP_FILE_LOCATION;
        File file = new File(this.storeLocation);
         // check if temp folder is present then delete
        if(file.exists())
        {
            try
            {
                MolapUtil.deleteFoldersAndFiles(file);
            }
            catch(MolapUtilException e)
            {
                throw new MolapSortKeyAndGroupByException("Problem while deleting the old temp directory", e);
            }
        }
        // create new temp folder
        if(!file.mkdirs())
        {
            throw new MolapSortKeyAndGroupByException("Problem while creating group by temp directory");
        }
        this.tmpFile = new File(this.storeLocation + File.separator + tableName + System.nanoTime());
        try
        {
            // open output stream on temop file 
            this.writeStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpFile), 50000));
        }
        catch(FileNotFoundException e)
        {
            throw new MolapSortKeyAndGroupByException("Problem while creating group by temp file", e);
        }

    }
    
    //TODO SIMIAN

    /**
     * Below method will be used to add new row 
     * 
     * @param row
     *
     */
    private void addNewRow(Object[] row) 
    {
        int indx = 0;
        // create new row based on column index mapping 
        Object[] newRow = new Object[columnIndexMapping.length + 1];
        
        for(int i = 0;i < columnIndexMapping.length;i++)
        {
            // if agg type is count and row [i] is not null hen count will be 1
            // otherwise 0
            if(this.aggType[i].equals(MolapCommonConstants.COUNT))
            {
                if(row[columnIndexMapping[i]] != null)
                {
                    newRow[indx++] = 1D;
                }
                else
                {
                    newRow[indx++] = 0D;
                }
            }
            // this check is for agg type sum, avg , dis-cont
            else if(!this.aggType[i].equals(MolapCommonConstants.MAX)
                    && !this.aggType[i].equals(MolapCommonConstants.MIN))
            {
                // if row[i] is not null then set the actual value otherwise set 0
                if(null != row[columnIndexMapping[i]])
                {
                    newRow[indx] = row[columnIndexMapping[i]];
                    indx++;
                }
                else
                {
                    newRow[indx++] = 0D;
                }
            }
            else
            {
                // for max and min set row[i]
                newRow[indx] = row[columnIndexMapping[i]];
                indx++;
            }
        }
        prvKey = (byte[])row[this.keyIndex];
        newRow[indx] = prvKey;
        prvRow = newRow;
        calculateMax(prvRow);
        calculateMin(prvRow);
        setDecimals(prvRow);
        calculateUnique();
    }

    /**
     * This method will be used to update the column index mapping array which
     * will be used for mapping actual column with row column
     * 
     * @param rowMeasureName
     *            row Measure Name
     * @param actualMeasures
     *            actual Measures
     * 
     */
    private void updateColumnIndexMapping(String[] rowMeasureName, String[] actualMeasures)
    {
        int index = 0;
        for(int i = 0;i < actualMeasures.length;i++)
        {
            for(int j = 0;j < rowMeasureName.length;j++)
            {
                if(actualMeasures[i].equals(rowMeasureName[j]))
                {
                    this.columnIndexMapping[index++] = j;
                    break;
                }
            }
        }
    }

    /**
     * This method will be used to add new row it will check if new row and
     * previous row key is same then it will merger the measure values, else it
     * return the previous row
     * 
     * @param row
     *            new row
     * @return previous row
     * @throws MolapSortKeyAndGroupByException
     * 
     */
    public void add(Object[] row) throws MolapSortKeyAndGroupByException
    {
        // if previous key is same and new key then we have to update the measure value based on aggregator 
        if(MolapDataProcessorUtil.compare(prvKey, (byte[])row[this.keyIndex]) == 0)
        {
            updateMeasureValue(row);
        }
        // else write the previous record to file and update the previous row new row 
        else
        {
            writeDataToFile(prvRow);
            addNewRow(row);
        }
    }

    /**
     * This method will be used to write the aggregated data to file For measure
     * value first it will write first boolean whether it is null or not and if
     * it not null then it will write the measure value Null check is not
     * required for mdkey because mdkey cannot be null if it null then check in
     * previous step
     * 
     * @throws MolapSortKeyAndGroupByException
     *             problem while writing
     */
    private void writeDataToFile(Object[] row) throws MolapSortKeyAndGroupByException
    {
        try
        {
            for(int i = 0;i < row.length - 1;i++)
            {
                // first writing whether measure value is null or not 
                writeStream.writeBoolean(row[i] != null);
                if(null != row[i])
                {
                    // if measure value is not null then only writing the measure value 
                    writeStream.writeDouble(((Double)row[i]).doubleValue());
                }
            }
            //writing the mdkey , mdkey cannnot be null if it is null then we need to check in previous step 
            writeStream.write((byte[])row[row.length - 1]);
            this.numberOfEntries++;
        }
        catch(IOException e)
        {
            throw new MolapSortKeyAndGroupByException("Problem while writing file", e);
        }
    }

    /**
     * This method will be used to start reading
     * 
     * @throws MolapSortKeyAndGroupByException
     *          problem in creating the input stream
     *
     */
    public void initiateReading() throws MolapSortKeyAndGroupByException
    {
        // write the last row
        writeDataToFile(prvRow);
        // close open out stream 
        MolapUtil.closeStreams(writeStream);
        try
        {
            // creating reading strem
            readingStream = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpFile), 100000));
        }
        catch(FileNotFoundException e)
        {
            throw new MolapSortKeyAndGroupByException("Problem while getting the ", e);
        }
    }

    /**
     * Below method will be used to check whether any more records are present in
     * the file to read or not
     * 
     * @return more records are present
     * 
     */
    public boolean hasNext()
    {
        return readCounter < numberOfEntries;
    }

    /**
     * Below method will be used to read the record from file
     * 
     * @return record
     * @throws MolapSortKeyAndGroupByException
     *          any problem while reading 
     *
     */
    public Object[] next() throws MolapSortKeyAndGroupByException
    {
        // return row will be of total number of measure + one mdkey
        Object[] outRow = new Object[aggType.length + 1];
        try
        {
            // reading first all the measure value from file 
            for(int i = 0;i < this.aggType.length;i++)
            {
                // if boolean is true than measure value was not null otherwise it was null
                if(readingStream.readBoolean())
                {
                    outRow[i] = readingStream.readDouble();
                }
            }
            // reading the mdkey
            byte[] mdKey = new byte[mdKeyLength];
            if(readingStream.read(mdKey) < 0)
            {
                LOGGER.error(
                        MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG,
                        "Problme while reading the Mdkey.");
            }
            // setting the mdkey 
            outRow[this.aggType.length] = mdKey;
            // increment the read counter;
            this.readCounter++;
        }
        catch(IOException e)
        {
            throw new MolapSortKeyAndGroupByException("Problem while reading the groupss temp file ", e);
        }
        // return row 
        return outRow;
    }

    /**
     * Below method will be used to delete the temp files and folders 
     * 
     */
    public void deleteTmpFiles()
    {
        try
        {
            // close all the open streams
            MolapUtil.closeStreams(readingStream);
            MolapUtil.closeStreams(writeStream);
            // delete folder and files 
            MolapUtil.deleteFoldersAndFiles(this.storeLocation);
        }
        catch(MolapUtilException e)
        {
            LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, e, e.getMessage());
        }
    }
    /**
     * This method will be used to update the measure value based on aggregator
     * type
     * 
     * @param row
     *            row
     * 
     */
    private void updateMeasureValue(Object[] row)
    {
        // update the measure value with new row measure value
        for(int i = 0;i < columnIndexMapping.length;i++)
        {
            aggregateValue(prvRow[i], row[columnIndexMapping[i]], aggType[i], i);
        }
        // calculate max
        calculateMax(prvRow);
        // calculate min
        calculateMin(prvRow);
        // calculate decimal
        setDecimals(prvRow);
        // calculate unique
        calculateUnique();
    }

    /**
     * This method will be used to update the measure value based on aggregator
     * type
     * 
     * @param object1
     *            previous row measure value
     * @param object2
     *            new row measure value
     * 
     * @param aggType
     * 
     * @param index
     *            measure index
     * 
     */
    private void aggregateValue(Object object1, Object object2, String aggType, int index)
    {
        // if aggregate type is max
        if(aggType.equals(MolapCommonConstants.MAX))
        {
            // if second in not null and first is null then update with second 
            if(null == object1 && null != object2)
            {
                prvRow[index] = object2;
            }
            // if first in not null and second is null then update with first 
            else if(null != object1 && null == object2)
            {
                prvRow[index] = object1;
            }
            // if both are not null than get max between both 
            else if(null != object1 && null != object2)
            {
                prvRow[index] = (Double)object1 > (Double)object2 ? object1 : object2;
            }
        }
        // if aggregate type is min
        else if(aggType.equals(MolapCommonConstants.MIN))
        {
            // if second in not null and first is null then update with second 
            if(null == object1 && null != object2)
            {
                prvRow[index] = object2;
            }
            // if first in not null and second is null then update with first 
            else if(null != object1 && null == object2)
            {
                prvRow[index] = object1;
            }
            // if both are not null than get min between both 
            else if(null != object1 && null != object2)
            {
                prvRow[index] = (Double)object1 < (Double)object2 ? object1 : object2;
            }
        }
        // if aggregate type is count 
        else if(aggType.equals(MolapCommonConstants.COUNT))
        {
            // if second is not null than increment the preious count value 
            if(null != object2)
            {
                double d = (Double)prvRow[index];
                d++;
                prvRow[index] = d;
            }
        }
        // if aggregate type is sum, avg, dis-count 
        else
        {
            // if second in not null and first is null then update with second 
            if(null == object1 && null != object2)
            {
                prvRow[index] = object2;
            }
            // if first in not null and second is null then update with first 
            else if(null != object1 && null == object2)
            {
                prvRow[index] = object1;
            }
            // if both are not null than get sum of both 
            else if(null != object1 && null != object2)
            {
                prvRow[index] = (Double)object1 + (Double)object2;
            }
        }
    }

    /**
     * This method will be used to update the max value for each measure
     * 
     * @param currentMeasures
     * 
     */
    private void calculateMax(Object[] row)
    {
        int arrayIndex = 0;
        for(int i = 0;i < row.length - 1;i++)
        {
            if(null!=row[i])
            {
                double value = (Double)row[i];
                maxValue[arrayIndex] = (maxValue[arrayIndex] > value ? maxValue[arrayIndex]
                        : value);
            }
            arrayIndex++;
        }
    }

    /**
     * This method will be used to update the max value for each measure
     * 
     * @param currentMeasures
     * 
     */
    private void calculateUnique()
    {
        for(int i = 0;i < this.aggType.length;i++)
        {
            if("max".equalsIgnoreCase(aggType[i]))
            {
                uniqueValue[i] = minValue[i] - 1;
            }
            else if("min".equalsIgnoreCase(aggType[i]))
            {
                uniqueValue[i] = maxValue[i] + 1;
            }
        }
    }

    /**
     * This method will be used to update the min value for each measure
     * 
     * @param currentMeasures
     * 
     */
    private void calculateMin(Object[] row)
    {
        int arrayIndex = 0;
        for(int i = 0;i < row.length - 1;i++)
        {
            if(null!=row[i])
            {
                double value = (Double)row[i];
                minValue[arrayIndex] = (minValue[arrayIndex] < value ? minValue[arrayIndex]
                        : value);
            }
            arrayIndex++;
        }
    }

    /**
     * This method will be used to update the measures decimal length If current
     * measure length is more then decimalLength then it will update the decimal
     * length for that measure
     * 
     * @param currentMeasure
     *            measures array
     * 
     */
    private void setDecimals(Object[] row)
    {
        int arrayIndex = 0;
        for(int i = 0;i < row.length - 1;i++)
        {
            if(null!=row[i])
            {
                double value = (Double)row[i];
                String measureString = String.valueOf(value);
                int index = measureString.indexOf(".");
                int num = 0;
                if(index != -1 && !"0".equalsIgnoreCase(measureString.substring(index + 1, measureString.length())))
                {
                    num = measureString.length() - index - 1;
                }
                decimalLength[arrayIndex] = (decimalLength[arrayIndex] > num ? decimalLength[arrayIndex] : num);
            }
            arrayIndex++;
        }
    }

    /**
     * Below method will be used to get the max value array for all the measures 
     * 
     * @return max value
     *
     */
    public double[] getMaxValue()
    {
        return maxValue;
    }

    /**
     * Below method will be used to get the min value array for all the measures 
     * 
     * @return min value
     *
     */
    public double[] getMinValue()
    {
        return minValue;
    }

    /**
     * Below method will be used to get the decimal length value array for all the measures 
     * 
     * @return decimal length
     *
     */
    public int[] getDecimalLength()
    {
        return decimalLength;
    }

    /**
     * Below method will be used to get the uinque value array for all the measures 
     * 
     * @return uinque length
     *
     */
    public double[] getUniqueValue()
    {
        return uniqueValue;
    }
}
