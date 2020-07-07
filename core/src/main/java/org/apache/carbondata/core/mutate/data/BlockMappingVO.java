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

package org.apache.carbondata.core.mutate.data;

import java.util.Map;

/**
 * VO class to store the details of segment and block count , block and its row count.
 */
public class BlockMappingVO {

  private Map<String, Long> blockRowCountMapping;

  private Map<String, Long> segmentNumberOfBlockMapping;

  private Map<String, RowCountDetailsVO> completeBlockRowDetailVO;

  // This map will help us to finding the segment id from the block path.
  // key is 'blockPath' and value is 'segmentId'
  private Map<String, String> blockToSegmentMapping;

  public void setCompleteBlockRowDetailVO(Map<String, RowCountDetailsVO> completeBlockRowDetailVO) {
    this.completeBlockRowDetailVO = completeBlockRowDetailVO;
  }

  public Map<String, RowCountDetailsVO> getCompleteBlockRowDetailVO() {
    return completeBlockRowDetailVO;
  }

  public Map<String, Long> getBlockRowCountMapping() {
    return blockRowCountMapping;
  }

  public Map<String, Long> getSegmentNumberOfBlockMapping() {
    return segmentNumberOfBlockMapping;
  }

  public BlockMappingVO(Map<String, Long> blockRowCountMapping,
      Map<String, Long> segmentNumberOfBlockMapping) {
    this.blockRowCountMapping = blockRowCountMapping;
    this.segmentNumberOfBlockMapping = segmentNumberOfBlockMapping;
  }

  public void setBlockToSegmentMapping(Map<String, String> blockToSegmentMapping) {
    this.blockToSegmentMapping = blockToSegmentMapping;
  }

  public Map<String, String> getBlockToSegmentMapping() {
    return blockToSegmentMapping;
  }
}
