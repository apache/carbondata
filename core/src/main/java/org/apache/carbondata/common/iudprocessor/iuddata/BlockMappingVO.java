package org.apache.carbondata.common.iudprocessor.iuddata;

import java.util.Map;

/**
 * VO class to store the details of segment and block count , block and its row count.
 */
public class BlockMappingVO {

  private Map<String, Long> blockRowCountMapping ;

  private Map<String, Long> segmentNumberOfBlockMapping ;

  private Map<String, RowCountDetailsVO> completeBlockRowDetailVO;

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
}
