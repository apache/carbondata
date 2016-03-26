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

package org.carbondata.query.cache;

public class FileBasedSegmentCache {
    //
    //    private final static ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory()
    //    {
    //        public Thread newThread(Runnable r)
    //        {
    //            final Thread thread = Executors.defaultThreadFactory().newThread(r);
    //            thread.setDaemon(true);
    //            thread.setName("com.huawei.unibi.molap.engine.cache.FileBasedSegmentCache$Executor"); //$NON-NLS-1$
    //            return thread;
    //        }
    //    });
    //
    ////    private final FileBasedSegmentCacheWorker worker;
    //    private final CustomFileBasedSegmentCacheWorker worker;
    //
    //    /**
    //     *
    //     */
    //    public FileBasedSegmentCache()
    //    {
    //        this.worker = new CustomFileBasedSegmentCacheWorker();
    //    }
    //
    //    /**
    //     *
    //     * @param worker
    //     */
    //    public FileBasedSegmentCache(CustomFileBasedSegmentCacheWorker worker)
    //    {
    //        this.worker = worker;
    //    }
    //
    //    /**
    //     * check contains
    //     * @param header
    //     * @return
    //     */
    //    public Future<Boolean> contains(final MolapSegmentHeader header)
    //    {
    //        return executor.submit(new Callable<Boolean>()
    //        {
    //            public Boolean call() throws Exception
    //            {
    //                return worker.contains(header);
    //            }
    //        });
    //    }
    //
    //    /**
    //     * get the body from cache
    //     * @param header
    //     * @return
    //     */
    //    public Future<MolapSegmentBody> get(final MolapSegmentHeader header)
    //    {
    //        return executor.submit(new Callable<MolapSegmentBody>()
    //        {
    //            public MolapSegmentBody call() throws Exception
    //            {
    //                return worker.get(header);
    //            }
    //        });
    //    }
    //
    //    /**
    //     * Get body without data
    //     * @param header
    //     * @return
    //     */
    //    public Future<MolapSegmentBody> getWithOutData(final MolapSegmentHeader header)
    //    {
    //        return executor.submit(new Callable<MolapSegmentBody>()
    //        {
    //            public MolapSegmentBody call() throws Exception
    //            {
    //                return worker.getWithoutData(header);
    //            }
    //        });
    //    }
    //
    //    /**
    //     * Get all headers for the cube
    //     * @param cubeName
    //     * @return
    //     */
    //    public Future<Set<MolapSegmentHeader>> getSegmentHeaders(final String cubeName)
    //    {
    //        return executor.submit(new Callable<Set<MolapSegmentHeader>>()
    //        {
    //            public Set<MolapSegmentHeader> call() throws Exception
    //            {
    //                return worker.getSegmentHeaders(cubeName);
    //            }
    //        });
    //    }
    //
    //    /**
    //     * Put the data to cache
    //     * @param header
    //     * @param body
    //     * @return
    //     */
    //    public Future<Boolean> put(final MolapSegmentHeader header, final MolapSegmentBody body)
    //    {
    //        return executor.submit(new Callable<Boolean>()
    //        {
    //            public Boolean call() throws Exception
    //            {
    //                return worker.put(header, body);
    //            }
    //        });
    //    }
    //
    //    /**
    //     * Remove the data from cache
    //     * @param header
    //     * @return
    //     */
    //    public Future<Boolean> remove(final MolapSegmentHeader header)
    //    {
    //        return executor.submit(new Callable<Boolean>()
    //        {
    //            public Boolean call() throws Exception
    //            {
    //                return worker.remove(header);
    //            }
    //        });
    //    }
    //
    //    /**
    //     * Flush the cache
    //     * @param region
    //     * @return
    //     */
    //    public Future<Boolean> flush(final ConstrainedColumn[] region)
    //    {
    //        return executor.submit(new Callable<Boolean>()
    //        {
    //            public Boolean call() throws Exception
    //            {
    //                return worker.flush(region);
    //            }
    //        });
    //    }
    //
    //    /**
    //     * Tear down complete cache
    //     */
    //    public void tearDown()
    //    {
    //        worker.tearDown();
    //    }
    //
    //
    //    /**
    //     * Flush the cube
    //     * @param schemaName
    //     * @param cubeName
    //     * @return
    //     */
    //    public Future<Boolean> flushCube(final String schemaName, final String cubeName)
    //    {
    //        return executor.submit(new Callable<Boolean>()
    //        {
    //            public Boolean call() throws Exception
    //            {
    //                return worker.flushCube(schemaName,cubeName);
    //            }
    //        });
    //    }
    //
    //    /**
    //     * Flush the cube with start key
    //     * @param cubeUniqueName
    //     * @param startKey
    //     * @param keyGen
    //     * @param tableName
    //     * @return
    //     */
    //    public Future<Boolean> flushCubeStartingWithKey(final String cubeUniqueName, final byte[] startKey, final KeyGenerator keyGen,final String tableName)
    //    {
    //        return executor.submit(new Callable<Boolean>()
    //                {
    //                    public Boolean call() throws Exception
    //                    {
    //                        return worker.flushCube(cubeUniqueName,startKey,keyGen,tableName);
    //                    }
    //                });
    //    }

}
