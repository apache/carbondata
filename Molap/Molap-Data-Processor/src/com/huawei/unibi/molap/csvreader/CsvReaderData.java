/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfnVI3c/udSMK6An9Lipq6FjccIMKj41/T4EBXl
K2tBN1ozIxAcrle3DGP2SXwfg4avqmZpxqd8tFBR5hd7VyALG53+pzjNpwVtriFsgE2onsE7
mxgHzESPfzitk0G245xYBxsawQs4+yasJ0MABiRKD/mrH+lQNUrLAdHY8oTroQ==*/
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
package com.huawei.unibi.molap.csvreader;

import org.pentaho.di.trans.steps.csvinput.CsvInputData;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :CsvReaderData.java 
 * Class Description :CsvReaderData 
 * Version 1.0
 */
public class CsvReaderData extends CsvInputData
{

    /**
     * totalBytesFilesFinished
     */
    public long totalBytesFilesFinished;
    
    /**
     * method to resize byte buffer array
     */
    @Override
    public void resizeByteBufferArray()
    {
        totalBytesFilesFinished=totalBytesFilesFinished+startBuffer;
        super.resizeByteBufferArray();
    }
}
