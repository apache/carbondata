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

package org.apache.carbondata.processing.merger;

import java.util.List;

import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.store.CarbonDataFileAttributes;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;

/**
 * This class contains the common methods required for result processing during compaction based on
 * restructure and normal scenarios
 */
public abstract class AbstractResultProcessor {

  /**
   * This method will perform the desired tasks of merging the selected slices
   *
   * @param resultIteratorList
   * @return
   */
  public abstract boolean execute(List<RawResultIterator> resultIteratorList);

  protected void setDataFileAttributesInModel(CarbonLoadModel loadModel,
      CompactionType compactionType, CarbonFactDataHandlerModel carbonFactDataHandlerModel) {
    CarbonDataFileAttributes carbonDataFileAttributes;
    if (compactionType == CompactionType.IUD_UPDDEL_DELTA) {
      long taskNo = CarbonUpdateUtil.getLatestTaskIdForSegment(loadModel.getSegmentId(),
          loadModel.getTablePath());
      // Increase the Task Index as in IUD_UPDDEL_DELTA_COMPACTION the new file will
      // be written in same segment. So the TaskNo should be incremented by 1 from max val.
      long index = taskNo + 1;
      carbonDataFileAttributes = new CarbonDataFileAttributes(index, loadModel.getFactTimeStamp());
    } else {
      carbonDataFileAttributes =
          new CarbonDataFileAttributes(Long.parseLong(loadModel.getTaskNo()),
              loadModel.getFactTimeStamp());
    }
    carbonFactDataHandlerModel.setCarbonDataFileAttributes(carbonDataFileAttributes);
  }

}
