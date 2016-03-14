/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfQVwqh74rUY6n+OZ2pUrkn1TkkvO60rFu08DZa
JnQq9KaUCidlHpqUsPNvYsck615G4tWhno+mCUhdkKGUp3kArDik6IAkXtyWdtlgIxcoWyif
IqINddejKMMBt2QiJ9L++2nquDUX9Q8ZPLXWGK1xK1C3XddQg+hTNyt0AjFGWw==*/
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
package com.huawei.unibi.molap.engine.scanner.optimizer;

/**
 * This interface is provided method to optimize the scanning in case we have filters applied.
 * 
 * @author R00900208
 * 
 */
public interface ScanOptimizer
{

    /**
     * This method returns the next key from where we need to start scan.
     * 
     * @param originalKey
     * @param transKey
     * @return
     *
     */
    byte[] getNextKey(long[] originalKey, byte[] transKey);

}
