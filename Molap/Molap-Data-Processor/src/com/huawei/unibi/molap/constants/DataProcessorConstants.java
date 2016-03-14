/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfnVI3c/udSMK6An9Lipq6FjccIMKj41/T4EBXl
K2tBN2Q3z3udVcEpuzJf5qiKBvkPYxW+GZ0hnnzETkrJTxYQondZL3xRtb04Rvu0FrIXb/Np
YTJSfESYshPbIu+5ABtYNhStoSG8vFvUovF+JmJ2myiuOPtTbdGf5QD8ncTh/Q==*/
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
package com.huawei.unibi.molap.constants;

/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :09-Jul-2013
 * FileName : DataProcessorConstants.java
 * Class Description : 
 * Version 1.0
 */
public final class DataProcessorConstants
{
	private DataProcessorConstants()
	{
		
	}
    /**
     * 
     */
    public static final String CSV_DATALOADER = "CSV_DATALOADER";

    /**
     * 
     */
    public static final String DATARESTRUCT = "DATARESTRUCT";
    
    /**
     * UPDATEMEMBER
     */
    public static final String UPDATEMEMBER= "UPDATEMEMBER";

    /**
     * number of days task should be in DB table
     */
    public static final String TASK_RETENTION_DAYS = "dataload.taskstatus.retention";
    
    /**
     * LOAD_FOLDER
     */
    public static final String LOAD_FOLDER = "Load_";

    /**
     * if bad record found 
     */
    public static final long BAD_REC_FOUND = 223732673;
    
    /**
     * if bad record found 
     */
    public static final long CSV_VALIDATION_ERRROR_CODE = 113732678;
    
    /**
     * Year Member val for data retention.
     */
    public static final String YEAR = "YEAR";
}
