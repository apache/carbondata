/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/oOyKngwpMgduBJvXsogYz9HAFLAnWmTBa4y5PT6lJw58O96saSu+27/sUKNAnl0QKEo
AgKCm/A7FwwSlitL1xxBhq2GcvZhChaSpDdUWaAst/dOk3F9iAXZhvQVdvhQ9A==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2014
 * =====================================
 *
 */
package com.huawei.unibi.molap.surrogatekeysgenerator.lru;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :LRUCacheCleanerSceduler.java 
 * Class Description : LRUCacheCleanerSceduler class 
 * Version 1.0
 */
public class LRUCacheCleanerSceduler //implements IPentahoSystemListener
{

//    private static final LogService LOGGER = LogServiceFactory.getLogService(MolapDataProcessorUtil.class.getName());
//    
//    /**
//     * executerService
//     */
//    private ScheduledExecutorService executerService;
//    
//    /**
//     * shutdown
//     */
//    public void shutdown()
//    {
//        if(null!=executerService)
//        {
//            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "SeqGen LRU Cache Cleaner Scheduler");
//            executerService.shutdownNow();
//        }
//    }
//
//    @Override
//    public boolean startup(IPentahoSession arg0)
//    {
//        MolapProperties instance = MolapProperties.getInstance();
//
//        boolean isMolapSeqGenCacheEnabled = Boolean
//                .parseBoolean(instance
//                        .getProperty(
//                                MolapCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED,
//                                MolapCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_ENABLED_DEFAULT_VALUE));
//        int cleanInterval = 0;
//        try
//        {
//            cleanInterval = Integer
//                    .parseInt(MolapProperties
//                            .getInstance()
//                            .getProperty(
//                                    MolapCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_FLUSH_INTERVAL_INHOUR,
//                                    MolapCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_FLUSH_INTERVAL_INHOUR_DEFAULTVALUE));
//        }
//        catch(NumberFormatException e)
//        {
//            cleanInterval = Integer
//                    .parseInt(MolapCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_FLUSH_INTERVAL_INHOUR_DEFAULTVALUE);
//        }
//        if(isMolapSeqGenCacheEnabled)
//        {
//            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Registering SeqGen LRU Cache Cleaner Scheduler");
//            LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "SeqGen LRU Cache Cleaner Delay Is: "+ cleanInterval);
//            executerService = Executors.newSingleThreadScheduledExecutor();
//            executerService.scheduleWithFixedDelay(
//                    new LRUCacheCleanerThread(cleanInterval), cleanInterval, cleanInterval, TimeUnit.MINUTES);
//        }
//        return true;
//    }

}
