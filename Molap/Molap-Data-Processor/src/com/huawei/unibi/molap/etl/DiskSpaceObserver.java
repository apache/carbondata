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
