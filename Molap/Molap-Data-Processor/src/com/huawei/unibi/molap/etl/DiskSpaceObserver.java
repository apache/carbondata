/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwddts1/q4bCGDA4M3dH8C2PEEMnfDqqdF4ZhcSc
1BeEnEw1DqEeXvIuNFstYZMvyV9h5RvDqbbDq0gPc+mYk0U56XNBJwu/P2y7M3gOynmxYqGW
V74yyFtjifmoGx5gc0kuHqKu26hlz4XiX/yJ4pP0TKXViNB6z1dLHpn4jmEwIg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.etl;


/**
 * Project Name NSE V3R7C00 
 * Module Name : MOLAP
 * Author :G70996
 * Created Date :18-Sep-2013
 * FileName : DiskSpaceObserver.java
 * Class Description : 
 * Version 1.0
 */
public class DiskSpaceObserver //implements ResourceOverloadObserver{
{
//	/**
//     * 
//     * Comment for <code>LOGGER</code>
//     * 
//     */
//    private static final LogService LOGGER = LogServiceFactory.getLogService(DiskSpaceObserver.class.getName());
//	
//	/**
//	 * This method will be called upon reaching the disk space threshold
//	 * It will kill all the data load tasks in-progress
//	 */
//	
//	public void performActionOnOverload(UniBIOverloadEvent uniBIOverloadEvent) {
//		
//		if(UniBIOverloadConstants.RESOURCE_HARD_DISK.equals(uniBIOverloadEvent.getType()))
//		{
//			LOGGER.info(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, uniBIOverloadEvent.getMsg());
//			try {
//				 CSVDataLoaderServiceHelper.getInstance().stopAllTasks();
//			} catch (ObjectFactoryException e) {
//				LOGGER.error(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "Unable to get instance of CSVDataLoaderServiceHelper");
//			}
//		}
//		
//	}
//
//	/**
//	 * This method will be called upon disk space coming down below threshold
//	 * 
//	 */
//	@Override
//	public void performActionOnOverloadRectification(UniBIOverloadEvent arg0) {
//		LOGGER.debug(MolapDataProcessorLogEvent.UNIBI_MOLAPDATAPROCESSOR_MSG, "disk space has come down below threshold");
//	}
}
