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
