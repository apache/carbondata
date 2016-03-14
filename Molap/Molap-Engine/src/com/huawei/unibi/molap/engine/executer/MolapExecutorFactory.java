/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOZqeL76MiMlg6E6zVw+6y7ByyVqdgfcu43v0iGbNX3+xHlbYmwO87lfkxXzUUIVzbyOJ
hUqeLRKBc7frlukSyQPBT7dbBm84wDHLcy7SDEZYqXD8mL3xqGlp4USliECqiQ==*/
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

package com.huawei.unibi.molap.engine.executer;

import java.io.IOException;
import java.util.List;

import javax.sql.DataSource;

import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;

/**
 * 
 * This class is factory for Molap Executor.
 * 
 * @author R00900208
 * 
 */
public final class MolapExecutorFactory
{

//    /**
//     * @param dimTables
//     * @param dataSource
//     * @return
//     * @throws IOException
//     */
//    public static MolapExecutor getMolapExecutor(List<Dimension> dimTables, DataSource dataSource) throws IOException
//    {
//        if(dataSource instanceof MolapDataSourceImpl)
//        {
//            return new InMemoryQueryExecutor(dimTables);
//        }
//        return null;
//    }
    private MolapExecutorFactory()
    {
        
    }
    
    /**
     * @param dimTables
     * @param dataSource
     * @return
     * @throws IOException
     */
    public static MolapExecutor getMolapExecutor(List<Dimension> dimTables, DataSource dataSource, String schemaName, String cubeName) throws IOException
    {
        return null;
    }

}
