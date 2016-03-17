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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdXNiZ+oxCgSX2SR8ePIzMmJfU7u5wJZ2zRTi4X
XHfqbWedH5fDmx9u8udkI9x68ELDs3Zro2EZzbahkt7zkayqO+Rv7GPRCzYKfQvW/e7jOLV0
dx1oahdItwJmCAPphBCg4wCrOg2DsjOxJe52gSFjejMagoh4jE2gWb6m/daNDQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.util;


/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :r70299
 * Created Date :Sep 3, 2013
 * FileName : IDataProcessTaskListener.java
 * Class Description : listener
 * Version 1.0
 */
public interface IDataProcessTaskListener {

	 /**
     * @param taskId
     * @param taskModel
     */
    void taskSuccessful(DataProcessTask dataProcessTask);

    /**
     * @param taskId
     * @param taskModel
     */
    void taskFailed(DataProcessTask dataProcessTask, String errMessage);
}
