/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.processing.loading.sort;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.sort.impl.ParallelReadMergeSorterImpl;
import org.apache.carbondata.processing.loading.sort.impl.ParallelReadMergeSorterWithColumnRangeImpl;
import org.apache.carbondata.processing.loading.sort.impl.UnsafeBatchParallelReadMergeSorterImpl;
import org.apache.carbondata.processing.loading.sort.impl.UnsafeParallelReadMergeSorterImpl;
import org.apache.carbondata.processing.loading.sort.impl.UnsafeParallelReadMergeSorterWithColumnRangeImpl;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;

public class SorterFactory {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SorterFactory.class.getName());

  public static Sorter createSorter(CarbonDataLoadConfiguration configuration, AtomicLong counter) {
    boolean offheapsort = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT,
            CarbonCommonConstants.ENABLE_UNSAFE_SORT_DEFAULT));
    SortScopeOptions.SortScope sortScope = CarbonDataProcessorUtil.getSortScope(configuration);
    Sorter sorter;
    if (offheapsort) {
      if (configuration.getBucketingInfo() != null) {
        sorter = new UnsafeParallelReadMergeSorterWithColumnRangeImpl(counter,
            configuration.getBucketingInfo());
      } else if (configuration.getSortColumnRangeInfo() != null) {
        sorter = new UnsafeParallelReadMergeSorterWithColumnRangeImpl(counter,
            configuration.getSortColumnRangeInfo());
      } else {
        sorter = new UnsafeParallelReadMergeSorterImpl(counter);
      }
    } else {
      if (configuration.getBucketingInfo() != null) {
        sorter = new ParallelReadMergeSorterWithColumnRangeImpl(counter,
            configuration.getBucketingInfo());
      } else if (configuration.getSortColumnRangeInfo() != null) {
        sorter = new ParallelReadMergeSorterWithColumnRangeImpl(counter,
            configuration.getSortColumnRangeInfo());
      } else {
        sorter = new ParallelReadMergeSorterImpl(counter);
      }
    }
    if (sortScope.equals(SortScopeOptions.SortScope.BATCH_SORT)) {
      if (configuration.getBucketingInfo() == null) {
        sorter = new UnsafeBatchParallelReadMergeSorterImpl(counter);
      } else {
        LOGGER.warn(
            "Batch sort is not enabled in case of bucketing. Falling back to " + sorter.getClass()
                .getName());
      }
    }
    return sorter;
  }

}
