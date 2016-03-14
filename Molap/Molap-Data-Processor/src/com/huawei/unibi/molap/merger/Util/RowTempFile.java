/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwe/owl+XpObKvwejIomJrN10iZBX17jBC5vj/zP
61+XabScfBjh/gIIZJi8qjshyEkkxS2aMp0rm7oLy2/87vzs/QDwzQVUBAH71pf4LW+cIRcP
XEn4phOXb/A6mIFpTh/y3/itqEU8qnik0Il6/aWMFNEORGp4g0p3fgSKdZQ58w==*/
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
package com.huawei.unibi.molap.merger.Util;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM
 * FileName : RowTempFile.java
 * Class Description :Class used for holding the data for sorting the records present in the file
 * Version 1.0
 */
public class RowTempFile
{
    /**
     * row
     */
    private byte[] row;

    /**
     * file size
     */
    private long fileSize;

    /**
     * offset 
     */
    private long offset;

    /**
     * file holder index
     */
    private int fileHolderIndex;

    /**
     *  file path
     */
    private String filePath;

    /**
     * RowTempFile constructor
     * 
     * @param row
     *      row
     * @param fileSize
     *      file size
     * @param offset
     *          offset
     * @param fileHolderIndex
     *          file holder index
     * @param filePath
     *          file path
     *
     */
    public RowTempFile(byte[] row, long fileSize, long offset, int fileHolderIndex, String filePath)
    {
        this.row = row;
        this.fileSize = fileSize;
        this.offset = offset;
        this.fileHolderIndex = fileHolderIndex;
        this.filePath = filePath;
    }
    
    /**
     * get the row
     * 
     * @return row
     * 
     */
    public byte[] getRow()
    {
        return row;
    }

    /**
     * Will return file size
     * 
     * @return file size
     *
     */
    public long getFileSize()
    {
        return fileSize;
    }

    /**
     *  Will return offset
     * 
     * @return offset
     *
     */
    public long getOffset()
    {
        return offset;
    }

    /**
     * Will return file holder index
     * 
     * @return fileHolderIndex
     *
     */
    public int getFileHolderIndex()
    {
        return fileHolderIndex;
    }

    /**
     * return the file path
     * 
     * @return filePath
     *
     */
    public String getFilePath()
    {
        return filePath;
    }

    /**
     * set the row
     * 
     * @param row
     *
     */
    public void setRow(byte[] row)
    {
        this.row = row;
    }

    /**
     * set the file size
     * 
     * @param fileSize
     *
     */
    public void setFileSize(long fileSize)
    {
        this.fileSize = fileSize;
    }

    /**
     * set the offset 
     * 
     * @param offset
     *
     */
    public void setOffset(long offset)
    {
        this.offset = offset;
    }

    /**
     * set the file holder index
     * 
     * @param fileHolderIndex
     *
     */
    public void setFileHolderIndex(int fileHolderIndex)
    {
        this.fileHolderIndex = fileHolderIndex;
    }

    /**
     * set the file path
     * 
     * @param filePath
     *
     */
    public void setFilePath(String filePath)
    {
        this.filePath = filePath;
    }


}
