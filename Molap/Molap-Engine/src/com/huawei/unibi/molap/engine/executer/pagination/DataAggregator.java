/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d/iakyPdveePvSA60n4x8wa+JYnwOVl/Jh+67YaZ7jfdLsODCEpltJU1q3Q0L14mz23h
cLDFOLLh6DcMj5lnnFrDpTuzgGMKWwNGfPhLcniXGbJt++gimSZkWozZslwrpA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination;

import com.huawei.unibi.molap.engine.scanner.Scanner;

/**
 * @author R00900208
 *
 */
public interface DataAggregator
{
    
    void aggregate(Scanner scanner);
    
    void finish() throws Exception;
    
    void interrupt();
    
}
