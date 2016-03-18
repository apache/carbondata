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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3XoRJqNfe0X9mVunjIRmYa4oEHiQ1KYP+THuQGjTYA0oXyvbKe6NFbuXfzzHyuYvNWsu
Op+4FzYY4BJGSE3jRzBbLGw5Q6fRjlvDszVcqsF6qbD9n37Rh/QSAdIuopBpAw==*/
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
package com.huawei.unibi.molap.engine.filters;

import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;

/**
 * Interface provided the methods to used in the scanning of the data store.
 * 
 * @author R00900208
 * 
 */
public interface InMemoryFilter
{
    /**
     * Return true if key is present or not in datastore. 
     * 
     * @param key
     * @return  Return true if key is present or not in datastore, false otherwise.
     *
     */
    boolean filterKey(KeyValue key);

    /**
     * Find the next node to scan the datastore.
     * 
     * @param key
     * @return
     *
     */
    byte[] getNextJump(KeyValue key);

}
