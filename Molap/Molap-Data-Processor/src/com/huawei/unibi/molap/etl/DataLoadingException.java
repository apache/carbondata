/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnGMLrB5I65/XL6xjm/txYAjm19L2BFwZT8E7IToTDEY/moDXEsf+AwU+oqCMFTzW5jGf
nwsU+rEF3frL4FiJaKdGpqZCEwHOKUOaS+QIjHLy+CQ5P6a++AjBo7uFKlG+QA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.etl;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :C00900810
 * Created Date :24-Jun-2013
 * FileName : DataLoadingException.java
 * Class Description : 
 * Version 1.0
 */
public class DataLoadingException extends Exception
{
    /**
   * 
   */
    private static final long serialVersionUID = 1L;

    /**
     * 
     */
    private long errorCode = -1;

    public DataLoadingException()
    {
        super();
    }

    public DataLoadingException(long errorCode,String message)
    {
        super(message);
        this.errorCode = errorCode;
    }

    public DataLoadingException(String message)
    {
        super(message);
    }

    public DataLoadingException(Throwable cause)
    {
        super(cause);
    }

    public DataLoadingException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * @return
     */
    public long getErrorCode()
    {
        return errorCode;
    }

}