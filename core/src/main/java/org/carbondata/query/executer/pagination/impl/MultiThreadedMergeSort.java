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

package org.carbondata.query.executer.pagination.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.query.executer.pagination.impl.DataFileWriter.KeyValueHolder;

public final class MultiThreadedMergeSort {
    private static ExecutorService executorService = Executors.newCachedThreadPool();

    private MultiThreadedMergeSort() {

    }

    /**
     * This method will be used for sorting
     *
     * @param src
     * @param comparator
     * @return
     * @throws Exception
     */
    public static KeyValueHolder[] sort(KeyValueHolder[] src, Comparator<KeyValueHolder> comparator)
            throws Exception {
        int numberOfThreads = 4;
        int equalParts = src.length / numberOfThreads;

        List<Future<SortThread>> sortThreads =
                new ArrayList<Future<SortThread>>(MolapCommonConstants.CONSTANT_SIZE_TEN);

        for (int i = 0; i < numberOfThreads; i++) {
            int srcPos = equalParts * i;
            if (i == (numberOfThreads - 1)) {
                if (srcPos + equalParts < src.length) {
                    equalParts = src.length - srcPos;
                }
                KeyValueHolder[] part = new KeyValueHolder[equalParts];
                System.arraycopy(src, srcPos, part, 0, equalParts);
                sortThreads.add(executorService
                        .submit(new SortThread(part, comparator, srcPos, srcPos + equalParts)));
            } else {
                KeyValueHolder[] part = new KeyValueHolder[equalParts];
                System.arraycopy(src, srcPos, part, 0, equalParts);
                sortThreads.add(executorService
                        .submit(new SortThread(part, comparator, srcPos, srcPos + equalParts)));
            }
        }
        List<KeyValueHolder[]> parts =
                new ArrayList<KeyValueHolder[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        int tasksCompleted = 0;
        try {
            //CHECKSTYLE:OFF    Approval No:Approval-249
            while (tasksCompleted != numberOfThreads) {//CHECKSTYLE:ON
                for (Future<SortThread> future : sortThreads) {
                    if (future.isDone()) {
                        tasksCompleted++;
                        parts.add(future.get().data);
                        //                                if(!resultInititalized)
                        //                                {
                        //
                        //                                    result = future.get();
                        //
                        //                                    resultInititalized = true;
                        //                                }
                        //                                else
                        //                                {
                        //                                    InMemoryQueryExecutor.mergeByteArrayMapResult(future.get(), result);
                        //                                }
                        sortThreads.remove(future);
                        break;
                    } else if (future.isCancelled()) {
                        tasksCompleted++;
                        sortThreads.remove(future);
                        break;
                    }
                }

                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            throw e;
        }

        List<Future<KeyValueHolder[]>> mergeFutures =
                new ArrayList<Future<KeyValueHolder[]>>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        mergeFutures.add(executorService
                .submit(new MergeThread(parts.get(0), parts.get(1), comparator)));
        mergeFutures.add(executorService
                .submit(new MergeThread(parts.get(2), parts.get(3), comparator)));
        parts.clear();
        List<KeyValueHolder[]> mergeParts =
                new ArrayList<KeyValueHolder[]>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        try {
            tasksCompleted = 0;
            //CHECKSTYLE:OFF    Approval No:Approval-249
            while (tasksCompleted != 2) {//CHECKSTYLE:ON
                for (Future<KeyValueHolder[]> future : mergeFutures) {
                    if (future.isDone()) {
                        tasksCompleted++;
                        mergeParts.add(future.get());
                        //                                  if(!resultInititalized)
                        //                                  {
                        //
                        //                                      result = future.get();
                        //
                        //                                      resultInititalized = true;
                        //                                  }
                        //                                  else
                        //                                  {
                        //                                      InMemoryQueryExecutor.mergeByteArrayMapResult(future.get(), result);
                        //                                  }
                        mergeFutures.remove(future);
                        break;
                    } else if (future.isCancelled()) {
                        tasksCompleted++;
                        mergeFutures.remove(future);
                        break;
                    }
                }

                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            throw e;
        }

        KeyValueHolder[] array1 = mergeParts.get(0);
        KeyValueHolder[] array2 = mergeParts.get(1);
        merge(src, array1, array2, comparator);
        return src;
    }

    /**
     * @param src
     * @param dest
     * @param low
     * @param high
     * @param off
     * @param c
     */
    private static void merge(KeyValueHolder[] src, KeyValueHolder[] array1,
            KeyValueHolder[] array2, Comparator c) {
        int arrayLength1 = array1.length;
        int arrayLength2 = array2.length;
        int srcLength = src.length;
        // Merge sorted halves (now in src) into dest
        int p = 0;
        int q = 0;
        for (int i = 0; i < srcLength; i++) {
            if (p < arrayLength1 && q < arrayLength2) {
                if (c.compare(array1[p], array2[q]) <= 0) {
                    src[i] = array1[p++];
                } else {
                    src[i] = array2[q++];
                }
            } else if (p < arrayLength1) {
                src[i] = array1[p++];
            } else if (q < arrayLength2) {
                src[i] = array2[q++];
            }
        }
    }

    private static final class SortThread implements Callable<SortThread> {

        private KeyValueHolder[] data;

        private Comparator<KeyValueHolder> comparator;

        // private int startPos;

        //  private int endPos;

        private SortThread(KeyValueHolder[] data, Comparator<KeyValueHolder> comparator,
                int startPos, int endPos) {
            this.data = data;
            this.comparator = comparator;
            // this.startPos = startPos;
            // this.endPos = endPos;
        }

        @Override public SortThread call() throws Exception {
            Arrays.sort(data, comparator);
            return this;
        }

    }

    private static final class MergeThread implements Callable<KeyValueHolder[]> {

        private KeyValueHolder[] array1;
        private KeyValueHolder[] array2;

        private Comparator<KeyValueHolder> comparator1;

        private MergeThread(KeyValueHolder[] array1, KeyValueHolder[] array2,
                Comparator<KeyValueHolder> comparator) {
            this.array1 = array1;
            this.array2 = array2;
            this.comparator1 = comparator;
        }

        @Override public KeyValueHolder[] call() throws Exception {
            KeyValueHolder[] src = new KeyValueHolder[array1.length + array2.length];
            merge(src, array1, array2, comparator1);
            return src;
        }

    }

    //    public static void main(String[] args) throws Exception
    //    {
    //
    //        Random random = new Random();
    //
    //        KeyValueHolder[] ds = new KeyValueHolder[10000000];
    //        for(int i = 0;i < ds.length;i++)
    //        {
    //            ds[i] = new KeyValueHolder(random.nextInt(ds.length));
    //        }
    //        long st = System.currentTimeMillis();
    ////        KeyValueHolder[] sort = sort(ds, new IntegerCompartor());
    //        Arrays.sort(ds, new IntegerCompartor());
    ////        System.out.println(sort.length);
    //        System.out.println((System.currentTimeMillis()-st));
    //    }
    //
    //    private static class IntegerCompartor implements Comparator<KeyValueHolder>
    //    {
    //
    //        @Override
    //        public int compare(KeyValueHolder o1, KeyValueHolder o2)
    //        {
    //            return o1.integer.compareTo(o2.integer);
    //        }
    //
    //    }

}
