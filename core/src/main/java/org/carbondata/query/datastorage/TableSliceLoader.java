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

package org.carbondata.query.datastorage;

/**
 * Creates incremental cube slice from differential incremental files and loads
 * into In-Memory
 * As of now, Cube merge is used with following rules to perform merge.
 * Allowed slice count is 4 including main cube. So, if any cube reaches this
 * count, a merge will be triggered to merge a possible small/latest slice to
 * one big slice with available incremental data.
 * If the biggest slice size if near or less ~10% in size of main cube, it can
 * be merged to the main cube by blocking all the queries.
 * Note: Only out place merge is encouraged as of now to keep the queries
 * un-blocked.
 *
 * @author K00900207
 */
public final class TableSliceLoader {
    //    /**
    //     *
    //     */
    //    //public static int MAX_SLICE_COUNT = 4;
    //
    //    /**
    //     * Attribute for Molap LOGGER
    //     */
    //    private static final LogService LOGGER = LogServiceFactory
    //            .getLogService(CubeSliceLoader.class.getName());
    //
    //    /**
    //     * Loads the slice (incremental cube) from give folder location.
    //     *
    //     * @param cubeName
    //     * @param filesLocaton
    //     */
    //    public void loadSliceFromFiles(String filesLocaton)
    //    {
    //        //
    //        long currentTimeMillis = System.currentTimeMillis();
    //        if(!(new File(filesLocaton).isDirectory()))
    //        {
    //
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                    "Unable to load cube slice as directory doesn't exist : " +
    //                    filesLocaton);
    //            return;
    //        }
    //        // Get CubeName, tableName, rsFolderName and path from filesLocaton
    //        CubeSlicePathInfo cubeSlicePathInfo = new CubeSlicePathInfo(filesLocaton);
    //        String rsFolderPath = cubeSlicePathInfo.getRsPath();
    //        String tableName = cubeSlicePathInfo.getTableName();
    //        String rsFolderName= cubeSlicePathInfo.getRsFolder();
    //        String cubeUniqueName=cubeSlicePathInfo.getCubeUniqueName();
    //        String tableFolderPath= rsFolderPath+File.separator+tableName;
    //        //
    //
    //        InMemoryCubeStore inMemoryStore = InMemoryCubeStore.getInstance();
    //
    //        RolapCube rolapCube = inMemoryStore.getRolapCube(cubeUniqueName);
    //
    //        if(null == rolapCube)
    //        {
    //            rolapCube = RolapUtil.getCube(cubeSlicePathInfo.getSchemaName(), cubeSlicePathInfo.getCubeName());
    //        }
    //        // It is possible that request has come very before rolapCube is formed.
    //        // This will result in rolapCube being null.
    //        // In this case, continuously try to get it after sleeping (blocking) for 0.5 sec.
    //        // Max 0.5 x 200 = 100 secs wait, come out of loop even if rolapCube null
    //        int maxTimeOut = 200;
    //        int count = 0;
    //        try
    //        {
    //            while(null == rolapCube && count < maxTimeOut)
    //            {
    //                Thread.sleep(500);
    //                rolapCube = inMemoryStore.getRolapCube(cubeUniqueName);
    //                count++;
    //            }
    //        }
    //        catch(InterruptedException e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                    e, "Thread Interrupted.");
    //        }
    //        // Tried but rolapCube not found
    //        if(null == rolapCube)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Error while slice loading : Unable to load cube : "+cubeUniqueName);
    //            return;
    //        }
    //
    //        InMemoryCube newSlice = new InMemoryCube(rolapCube);
    //        //
    //
    //
    //        RestructureStore rsStore = inMemoryStore.findRestructureStore(cubeUniqueName,rsFolderName);
    //        if(rsStore == null)
    //        {
    //            rsStore = new RestructureStore(rsFolderName);
    ////            rsStore.setFolderName(rsFolderName);
    //            inMemoryStore.registerSlice(newSlice,rsStore);
    //        }
    //
    //        rsStore.setSliceMetaCache(readSliceMetaDataFile(tableFolderPath), tableName);
    //        newSlice.setRsStore(rsStore);
    //        newSlice.loadCacheFromFile(filesLocaton,tableName);
    //        rsStore.setSlice(newSlice, tableName);
    //        if(null != newSlice.getDataCache(cubeSlicePathInfo.getTableName()))
    //        {
    //            ((MolapSchema)rolapCube.getSchema()).setCacheChanged(true);
    //        MolapCacheManager.getInstance().flushCubeStartingWithKey(cubeSlicePathInfo.getCubeUniqueName(),
    //                newSlice.getStartKey(cubeSlicePathInfo.getTableName()),
    //                newSlice.getKeyGenerator(cubeSlicePathInfo.getTableName()),cubeSlicePathInfo.getTableName());
    //        }
    //        LOGGER.debug(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                "Time Taken to load new slice: "
    //                + (System.currentTimeMillis() - currentTimeMillis));
    //    }
    //
    //    private SliceMetaData readSliceMetaDataFile(String path)
    //    {
    //        //
    //        SliceMetaData readObject=null;
    //        InputStream stream = null;
    //        ObjectInputStream objectInputStream = null;
    //        //
    //        try
    //        {
    //            stream = FileFactory.getDataInputStream(path + File.separator + MolapCommonConstants.SLICE_METADATA_FILENAME, FileFactory.getFileType());//new FileInputStream(path + File.separator + MolapCommonConstants.SLICE_METADATA_FILENAME);
    //            objectInputStream = new ObjectInputStream(stream);
    //            readObject = (SliceMetaData)objectInputStream.readObject();
    //        }
    //        catch(ClassNotFoundException e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e.getMessage());
    //        }
    //        //
    //        catch (IOException e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e.getMessage());
    //        }
    //        finally
    //        {
    //            MolapUtil.closeStreams(objectInputStream,stream);
    //        }
    //        return readObject;
    //    }
    //
    //    /**
    //     * Identifies the slice to be merged with latest available data. If there is
    //     * no need to merge, it can give null.
    //     *
    //     * @return
    //     */
    //    public InMemoryCube identifySliceToMerge(String cubeName)
    //    {
    ////        List<InMemoryCube> allSlices = InMemoryCubeStore.getInstance().getActiveSlices(cubeName);
    ////
    ////        if(allSlices.size() >= MAX_SLICE_COUNT)
    ////        {
    ////            // If the slice count reaches to maximum no. get the smallest slice
    ////            // and merge this into it.
    ////            InMemoryCube smallSlice = allSlices.get(0);
    ////            long size = smallSlice.getSize();
    ////            for(InMemoryCube slice : allSlices)
    ////            {
    ////                if(size > slice.getSize())
    ////                {
    ////                    smallSlice = slice;
    ////                }
    ////            }
    ////
    ////            return smallSlice;
    ////        }
    //
    //        return null;
    //    }
    //
    //    // /**
    //    // * Merges the given slices and registers required listeners for clean up
    //    // * activity.
    //    // *
    //    // * @param slice1
    //    // * @param slice2
    //    // */
    //    // private void outPlaceMerge(InMemoryCube slice1, InMemoryCube slice2)
    //    // {
    //    // InMemoryCube newCopy = new InMemoryCube(slice1.getRolapCube());
    //    //
    //    // newCopy.loadCache(slice1, false);
    //    // newCopy.loadCache(slice2, true);
    //    //
    //    // // Register slice listeners for each slice
    //    // List<Long> queries = QueryMapper.getQueriesPerSlice(slice1);
    //    // SliceListener listener = new SliceListener(slice1);
    //    // for(Long query : queries)
    //    // {
    //    // listener.registerQuery(query);
    //    // QueryMapper.registerSliceListener(listener, query);
    //    // }
    //    //
    //    // SliceListener listener2 = new SliceListener(slice2);
    //    // for(Long query : queries)
    //    // {
    //    // listener2.registerQuery(query);
    //    // QueryMapper.registerSliceListener(listener2, query);
    //    // }
    //    //
    //    // // Mark the slices inactive and add new slice to the cube
    //    // slice1.setCubeMerged();
    //    // slice2.setCubeMerged();
    //    // InMemoryCubeStore.getInstance().registerSlice(slice1.getCubeName(),
    //    // newCopy);
    //    // }
    //
    //    /**
    //     * Makes an out place merge of given slices.
    //     *
    //     * @param newSlice
    //     *
    //     * @return
    //     */
    //    // private void mergeAndLoadSlice(List<InMemoryCube> sourceSlices, String
    //    // fileLocation, InMemoryCube newSlice)
    //    // {
    //    // // First load the cube
    //    // newSlice.loadCache(sourceSlices, fileLocation);
    //    //
    //    // for(InMemoryCube source : sourceSlices)
    //    // {
    //    // List<Long> queries = QueryMapper.getQueriesPerSlice(source);
    //    //
    //    // // Register slice listeners for each slice if there are any
    //    // // dependent
    //    // // queries on the source slice. Then listener will take care of
    //    // // cleaning
    //    // // the slice.
    //    // if(queries != null && queries.size() > 0)
    //    // {
    //    // SliceListener listener = new SliceListener(source);
    //    // for(Long query : queries)
    //    // {
    //    // listener.registerQuery(query);
    //    // QueryMapper.registerSliceListener(listener, query);
    //    // }
    //    // }
    //    // // Else, means no dependencies on slice. So, it can be cleaned
    //    // else
    //    // {
    //    // // Yes this slice is ready to clear
    //    // InMemoryCubeStore.getInstance().unRegisterSlice(source.getCubeName(),
    //    // source);
    //    // source.clean();
    //    // }
    //    //
    //    // // Mark the slices inactive and add new slice to the cube
    //    // source.setCubeMerged();
    //    // }
    //    // }
    //
    //    /**
    //     * background merge slices, used by action sequence.
    //     *
    //     * @param cubeName
    //     *            added by liupeng 00204190
    //     */
    //    public void backgroundMergeSlices(String cubeName)
    //    {
    ////        List<InMemoryCube> allSlices = InMemoryCubeStore.getInstance().getActiveSlices(cubeName);
    ////
    ////        // it will merge slices when slice number is more than 1.
    ////        // if slice number is 1, no need to merge.
    ////        if(allSlices.size() > 1)
    ////        {
    ////            InMemoryCube slice1 = allSlices.get(0);
    ////
    ////            InMemoryCube newCopy = new InMemoryCube(slice1.getRolapCube());
    ////
    ////            newCopy.loadCache(allSlices, null);
    ////
    ////            for(InMemoryCube slice : allSlices)
    ////            {
    ////                SliceListener listener = new SliceListener(slice);
    ////                List<Long> queries = QueryMapper.getQueriesPerSlice(slice);
    ////                for(Long query : queries)
    ////                {
    ////                    listener.registerQuery(query);
    ////                    QueryMapper.registerSliceListener(listener, query);
    ////                }
    ////
    ////                slice.setCubeMerged();
    ////            }
    ////
    ////            InMemoryCubeStore.getInstance().registerSlice(newCopy.getCubeName(), newCopy);
    ////        }
    //    }
    //
    //    private CubeSliceLoader()
    //    {
    //
    //    }
    //
    //    /**
    //     *
    //     */
    //    private static CubeSliceLoader instance = new CubeSliceLoader();
    //
    //    /**
    //     * Create instance for calling of action sequence.
    //     *
    //     * @return added by liupeng 00204190
    //     */
    //    public static CubeSliceLoader getInstance()
    //    {
    //        return instance;
    //    }
    //
    //
    //    /**
    //     * 1. Form The new slice. <p>
    //     * 2. Take a list of loadFolders to unload. <p>
    //     * 3. Add the newly created slice. <p>
    //     * <p>
    //     * @param newSlicePath : New path to load from(new slice to form)
    //     * @param slicePathsToDelete : list of slice paths to unload
    //     *
    //     */
    //    public void updateSlices(String newSlicePath,String[] slicePathsToDelete,boolean flushFileCache, boolean needtoFlushCompleteCubeCache)
    //    {
    //        CubeSlicePathInfo cubeSlicePathInfo = null;
    //        InMemoryCube createdSlice = null;
    //        RestructureStore restructureStore = null;
    //
    //        //Validate and return after logging
    //        if(newSlicePath == null && slicePathsToDelete == null)
    //        {
    //            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "No update as path is null");
    //            return;
    //        }
    //
    //        //Some slice is to be added, create Slice for this
    //        if(newSlicePath != null)
    //        {
    //            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Trying to load new slice "
    //                    + newSlicePath);
    //            // form the new slice and set it
    //            // Don't add it though, we need to block queries for that
    //            // Otherwise we will get inconsistent results.
    //            cubeSlicePathInfo =   new CubeSlicePathInfo(newSlicePath);
    //            RolapCube rolapCube = RolapUtil.getCube(cubeSlicePathInfo.getSchemaName(), cubeSlicePathInfo.getCubeName());
    //         // It is possible that request has come very before rolapCube is formed.
    //            // This will result in rolapCube being null.
    //            // In this case, continuously try to get it after sleeping (blocking) for 0.5 sec.
    //            // Max 0.5 x 200 = 100 secs wait, come out of loop even if rolapCube null
    //            int maxTimeOut = 200;
    //            int count = 0;
    //            try
    //            {
    //                while(null == rolapCube && count < maxTimeOut)
    //                {
    //                    Thread.sleep(500);
    //                    rolapCube = RolapUtil.getCube(cubeSlicePathInfo.getSchemaName(),cubeSlicePathInfo.getCubeName());
    //                    count++;
    //                }
    //            }
    //            catch(InterruptedException e)
    //            {
    //                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                        e, "Thread Interrupted.");
    //            }
    //            // Tried but rolapCube not found
    //            if(null == rolapCube)
    //            {
    //                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Error while slice loading : Unable to load cube : "+cubeSlicePathInfo.getCubeUniqueName());
    //                return;
    //            }
    //            deleteSliceFromCube(cubeSlicePathInfo.getLoadPath());
    //            createdSlice = new InMemoryCube(rolapCube);
    //            restructureStore = InMemoryCubeStore.getInstance().findRestructureStore(cubeSlicePathInfo.getCubeUniqueName(), cubeSlicePathInfo.getRsFolder());
    //            createdSlice.setRsStore(restructureStore);
    //            createdSlice.loadCacheFromFile(cubeSlicePathInfo.getLoadPath(),cubeSlicePathInfo.getTableName());
    //            if(flushFileCache && null != createdSlice.getDataCache(cubeSlicePathInfo.getTableName())
    //                    && !needtoFlushCompleteCubeCache)
    //            {
    //                MolapCacheManager.getInstance().flushCubeStartingWithKey(cubeSlicePathInfo.getCubeUniqueName(),
    //                        createdSlice.getStartKey(cubeSlicePathInfo.getTableName()),
    //                        createdSlice.getKeyGenerator(cubeSlicePathInfo.getTableName()),
    //                        cubeSlicePathInfo.getTableName());
    //            }
    //            else if(null != createdSlice.getDataCache(cubeSlicePathInfo.getTableName()) && needtoFlushCompleteCubeCache)
    //            {
    //                MolapCacheManager.getInstance().flushCube(cubeSlicePathInfo.getSchemaName(),
    //                        cubeSlicePathInfo.getCubeName());
    //            }
    //        }
    //        else if(slicePathsToDelete != null)//Added check for Coverity
    //        {
    //            // no slice to add,
    //            // form cubeSlicePathInfo using first element of slicePathsToDelete array
    //            cubeSlicePathInfo =   new CubeSlicePathInfo(slicePathsToDelete[0]);
    //        }
    //        else
    //        {
    //            return;
    //        }
    //
    //        updateNewSlice(newSlicePath, slicePathsToDelete, cubeSlicePathInfo, createdSlice, restructureStore);
    //
    //    }
    //
    //    private void updateNewSlice(String newSlicePath, String[] slicePathsToDelete, CubeSlicePathInfo cubeSlicePathInfo,
    //            InMemoryCube createdSlice, RestructureStore restructureStore)
    //    {
    //        // Put queries in Wait state, so that new queries will wait
    //        InMemoryCubeStore.getInstance().setQueryExecuteStatus(cubeSlicePathInfo.getCubeUniqueName(),
    //                InMemoryCubeStore.QUERY_WAITING);
    //
    //        // Wait till existing queries are finished
    //        // Wait max 5 minutes - this is to avoid infinite loop
    //        int maxTimeOut = 600;
    //        int count = 0;
    //        try
    //        {
    //            while(QueryMapper.getActiveQueriesCount(cubeSlicePathInfo.getCubeUniqueName()) != 0
    //                    && count < maxTimeOut)
    //            {
    //                Thread.sleep(500);
    //                count++;
    //            }
    //        }
    //        catch(InterruptedException e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                    e, "Thread Interrupted while waiting for queries to execute.");
    //        }
    //
    //        // All existing queries have now finished execution
    //        // Now we can safely delete and add new slice
    //        if(slicePathsToDelete != null)
    //        {
    //            for(String toDeleteSlice : slicePathsToDelete)
    //            {
    //                // delete slice from memory
    //                deleteSliceFromCube(toDeleteSlice);
    //                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                        "Deleted slice: " + toDeleteSlice);
    //            }
    //        }
    //
    //        //If a new slice is to be added, add it
    //        if(createdSlice != null && restructureStore!= null)
    //        {
    //            restructureStore.setSlice(createdSlice,
    //                    cubeSlicePathInfo.getTableName());
    ////            InMemoryCubeStore.getInstance().registerSlice(cubeSlicePathInfo.getCubeName(),
    ////                    createdSlice,restructureStore);
    //            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                    "Updated load from " + newSlicePath);
    //        }
    //
    //        //Release the wait, so that queries can be executed again.
    //        InMemoryCubeStore.getInstance().setQueryExecuteStatus(cubeSlicePathInfo.getCubeUniqueName(),
    //                InMemoryCubeStore.QUERY_AVAILABLE);
    //
    //        try
    //        {
    //            // Now Delete slicePathsToDelete from persistent data store as well
    //            if(null != slicePathsToDelete)
    //            {
    //                MolapUtil.deleteFoldersAndFiles(slicePathsToDelete);
    //            }
    //
    //
    //        }
    //        catch(MolapUtilException e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                    "Problem while delete the olde slices", e);
    //        }
    //    }
    //
    //    /**
    //     * 1. Form The new slice. <p>
    //     * 2. Take a list of loadFolders to unload. <p>
    //     * 3. Add the newly created slice. <p>
    //     * <p>
    //     * @param newSlicePath : New path to load from(new slice to form)
    //     * @param slicePathsToDelete : list of slice paths to unload
    //     *
    //     */
    //    public void deleteSlices(String[] slicePathsToDelete)
    //    {
    //        CubeSlicePathInfo cubeSlicePathInfo = null;
    //        cubeSlicePathInfo =   new CubeSlicePathInfo(slicePathsToDelete[0]);
    //        //Some slice is to be added, create Slice for this
    //        // Put queries in Wait state, so that new queries will wait
    //        InMemoryCubeStore.getInstance().setQueryExecuteStatus(cubeSlicePathInfo.getCubeUniqueName(),
    //                InMemoryCubeStore.QUERY_WAITING);
    //
    //        // Wait till existing queries are finished
    //        // Wait max 5 minutes - this is to avoid infinite loop
    //        int maxTimeOut = 600;
    //        int count = 0;
    //        try
    //        {
    //            while(QueryMapper.getActiveQueriesCount(cubeSlicePathInfo.getCubeUniqueName()) != 0
    //                    && count < maxTimeOut)
    //            {
    //                Thread.sleep(500);
    //                count++;
    //            }
    //        }
    //        catch(InterruptedException e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                    e, "Thread Interrupted while waiting for queries to execute.");
    //        }
    //
    //        // All existing queries have now finished execution
    //        // Now we can safely delete and add new slice
    ////        if(slicePathsToDelete != null)
    ////        {
    //            for(String toDeleteSlice : slicePathsToDelete)
    //            {
    //                // delete slice from memory
    //                deleteSliceFromCube(toDeleteSlice);
    //                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                        "Deleted slice: " + toDeleteSlice);
    //            }
    ////        }
    //
    //        //Release the wait, so that queries can be executed again.
    //        InMemoryCubeStore.getInstance().setQueryExecuteStatus(cubeSlicePathInfo.getCubeUniqueName(),
    //                InMemoryCubeStore.QUERY_AVAILABLE);
    //    }
    //
    //    /**
    //     * path of the slice to unload
    //     * <p>
    //     * @param loadPath
    //     */
    //    public void deleteSliceFromCube(String loadPath)
    //    {
    //        //
    //        CubeSlicePathInfo cubeSlicePathInfo = new CubeSlicePathInfo(loadPath);
    //        RestructureStore restructureStore = InMemoryCubeStore.getInstance().findRestructureStore(
    //                cubeSlicePathInfo.getCubeUniqueName(), cubeSlicePathInfo.getRsFolder());
    //        if( null != restructureStore )
    //        {
    //            List<InMemoryCube> slices = restructureStore.getSlices(cubeSlicePathInfo.getTableName());
    //            InMemoryCube sliceToDelete = null;
    //            for(InMemoryCube slice : slices)
    //            {
    //                // Slices match based on the load folder name
    //                if(slice.getCubeSlicePathInfo().getLoadFolder().equals(cubeSlicePathInfo.getLoadFolder()))
    //                {
    //                    sliceToDelete = slice;
    //                    break;
    //                }
    //            }
    //            //
    //            InMemoryCubeStore.getInstance().unRegisterSlice(cubeSlicePathInfo.getCubeUniqueName(), sliceToDelete);
    //            restructureStore.removeSlice(sliceToDelete, cubeSlicePathInfo.getTableName());
    //            if(sliceToDelete != null && null != sliceToDelete.getDataCache(cubeSlicePathInfo.getTableName()))
    //            {
    //                MolapCacheManager.getInstance().flushCubeStartingWithKey(cubeSlicePathInfo.getCubeUniqueName(),
    //                        sliceToDelete.getStartKey(cubeSlicePathInfo.getTableName()),
    //                        sliceToDelete.getKeyGenerator(cubeSlicePathInfo.getTableName()),cubeSlicePathInfo.getTableName());
    //            }
    //        }else
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "deleteSliceFromCube: restructureStore is null so not deleted "+ loadPath);
    //        }
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Deleted slice from " + loadPath);
    //    }
    //
    //    /**
    //     * This is to update the cache after update member
    //     * @param sliceLoadPath
    //     */
    //    public void updateSchemaHierarchy(String sliceLoadPath)
    //    {
    //        CubeSlicePathInfo cubeSlicePathInfo = new CubeSlicePathInfo(sliceLoadPath);
    //        RolapCube cube = RolapUtil.getCube(cubeSlicePathInfo.getSchemaName(), cubeSlicePathInfo.getCubeName());
    //        /**
    //         * Fortify Fix: NULL_RETURNS
    //         */
    //        if(null != cube)
    //        {
    //            ((MolapSchema)cube.getSchema()).setCacheChanged(true);
    //        }
    //        updateSlices(sliceLoadPath, null,false, false);
    //    }
}
